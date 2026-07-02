package com.wife.app;

import android.content.Context;
import android.content.Intent;
import android.os.Build;
import android.os.Environment;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;

import androidx.localbroadcastmanager.content.LocalBroadcastManager;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GroupFileReceiver {
    private static final String TAG = "GroupFileReceiver";

    public interface GroupFileReceiveListener {
        void onProgress(String filename, int percent);
        void onComplete(String filename, String localPath);
        void onError(String error);
    }

    private static final List<GroupFileReceiveListener> listeners = new ArrayList<>();
    private static final ConcurrentHashMap<String, Long> lastBroadcastTimes = new ConcurrentHashMap<>();
    private static final ExecutorService receiverExecutor = Executors.newFixedThreadPool(5);

    public static synchronized void registerListener(GroupFileReceiveListener listener) {
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    public static synchronized void unregisterListener(GroupFileReceiveListener listener) {
        listeners.remove(listener);
    }

    /**
     * Symmetrical group server socket entry point.
     * Listens on Port 8905 and processes concurrent incoming multi-device streams.
     */
    public static void startServer(final Context context) {
        new Thread(() -> {
            ServerSocketChannel serverChannel = null;
            try {
                WifeLogger.log(TAG, "Opening ServerSocketChannel on group file port: " + Constants.OFF_PORT_GROUP_FILE);
                serverChannel = ServerSocketChannel.open();
                serverChannel.socket().bind(new InetSocketAddress(Constants.OFF_PORT_GROUP_FILE));
                WifeLogger.log(TAG, "Group ServerSocketChannel successfully bound. Entering persistent accept loop.");

                while (serverChannel.isOpen()) {
                    SocketChannel clientChannel = null;
                    try {
                        clientChannel = serverChannel.accept();
                        clientChannel.configureBlocking(true);

                        // High-Speed Socket Configurations on accepted client socket
                        clientChannel.socket().setTcpNoDelay(true);
                        clientChannel.socket().setReceiveBufferSize(1024 * 1024); // 1MB input buffer

                        final SocketChannel finalChannel = clientChannel;
                        
                        // Hand off accepted connection to the executor pool to support parallel workers safely
                        receiverExecutor.execute(() -> {
                            try {
                                String clientIp = finalChannel.socket().getInetAddress().getHostAddress();
                                WifeLogger.log(TAG, "Processing parallel group transfer stream connection from: " + clientIp);
                                
                                // Increment: Track this newly accepted socket connection as an active transaction
                                GroupFileTransferForegroundService.activeTransfersCount.incrementAndGet();

                                processGroupPersistentStream(context, finalChannel);
                            } catch (Exception e) {
                                WifeLogger.log(TAG, "Parallel group socket stream connection failed: " + e.getMessage(), e);
                                broadcastError(context, e.getMessage(), finalChannel.socket().getInetAddress().getHostAddress());
                            } finally {
                                try {
                                    finalChannel.close();
                                } catch (IOException ignored) {}
                                
                                // Centralized decrement handles safe teardown inside finally block
                                GroupFileTransferForegroundService.decrementAndCheckStop(context);
                            }
                        });

                    } catch (Exception e) {
                        WifeLogger.log(TAG, "Active group socket connection accept failed: " + e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                WifeLogger.log(TAG, "Group ServerSocketChannel threw exception or was closed: " + e.getMessage());
                if (serverChannel != null && serverChannel.isOpen()) {
                    broadcastError(context, e.getMessage());
                }
            } finally {
                try {
                    if (serverChannel != null) {
                        serverChannel.close();
                    }
                } catch (IOException ignored) {}
            }
        }).start();
    }

    /**
     * Processes metadata headers and LZ4 decompression segments sequentially over the active Group SocketChannel.
     */
    private static void processPersistentStream(Context context, SocketChannel socketChannel) throws Exception {
        GroupFileTransferForegroundService.isCancelled = false;
        GroupFileTransferForegroundService.isPaused = false;

        InputStream rawSocketIn = socketChannel.socket().getInputStream();
        NonClosingInputStream proxyIn = new NonClosingInputStream(rawSocketIn);
        int fileIndex = 0;
        String clientIp = socketChannel.socket().getInetAddress() != null ? socketChannel.socket().getInetAddress().getHostAddress() : "Unknown IP";

        while (!GroupFileTransferForegroundService.isCancelled && socketChannel.isConnected()) {
            byte[] lenBytes = new byte[4];
            int bytesRead = 0;
            while (bytesRead < 4 && !GroupFileTransferForegroundService.isCancelled) {
                synchronized (GroupFileTransferForegroundService.pauseLock) {
                    while (GroupFileTransferForegroundService.isPaused && !GroupFileTransferForegroundService.isCancelled) {
                        try {
                            WifeLogger.log(TAG, "Receiver thread entering wait state due to active pause command.");
                            GroupFileTransferForegroundService.pauseLock.wait();
                        } catch (InterruptedException e) {
                            WifeLogger.log(TAG, "Receiver pause monitor thread interrupted.");
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                if (GroupFileTransferForegroundService.isCancelled) {
                    break;
                }

                int read = proxyIn.read(lenBytes, bytesRead, 4 - bytesRead);
                if (read == -1) {
                    if (bytesRead == 0) {
                        WifeLogger.log(TAG, "Persistent group stream disconnected cleanly by sender IP [" + clientIp + "]. Closing queue.");
                        broadcastCompletion(context);
                    } else {
                        WifeLogger.log(TAG, "Stream ended abruptly while reading metadata length.");
                    }
                    return;
                }
                bytesRead += read;
            }

            if (GroupFileTransferForegroundService.isCancelled) {
                break;
            }

            int metadataLength = ((lenBytes[0] & 0xFF) << 24) |
                                 ((lenBytes[1] & 0xFF) << 16) |
                                 ((lenBytes[2] & 0xFF) << 8)  |
                                 (lenBytes[3] & 0xFF);

            if (metadataLength == 0) {
                WifeLogger.log(TAG, "End of persistent group queue stream marker received. Closing stream.");
                broadcastCompletion(context);
                break;
            }

            byte[] metaBytes = new byte[metadataLength];
            bytesRead = 0;
            while (bytesRead < metadataLength && !GroupFileTransferForegroundService.isCancelled) {
                synchronized (GroupFileTransferForegroundService.pauseLock) {
                    while (GroupFileTransferForegroundService.isPaused && !GroupFileTransferForegroundService.isCancelled) {
                        try {
                            GroupFileTransferForegroundService.pauseLock.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                if (GroupFileTransferForegroundService.isCancelled) {
                    break;
                }

                int read = proxyIn.read(metaBytes, bytesRead, metadataLength - bytesRead);
                if (read == -1) {
                    throw new IOException("Stream ended abruptly while reading metadata payload.");
                }
                bytesRead += read;
            }

            if (GroupFileTransferForegroundService.isCancelled) {
                break;
            }

            String metaJson = new String(metaBytes, StandardCharsets.UTF_8);
            JsonObject meta = com.google.gson.JsonParser.parseString(metaJson).getAsJsonObject();

            final String filename = meta.get("name").getAsString();
            final long originalSize = meta.get("size").getAsLong();
            final long compressedSize = meta.get("compressedSize").getAsLong();
            final boolean compress = meta.has("compressed") ? meta.get("compressed").getAsBoolean() : true;
            long resumePosition = meta.has("lastPosition") ? meta.get("lastPosition").getAsLong() : 0;

            WifeLogger.log(TAG, "Processing group incoming payload: " + filename + " | Size: " + originalSize + " bytes from sender: " + clientIp);

            File targetDir = getTargetDirectory(context, filename);
            File backupDir = getBackupDirectory();

            File fileDest = new File(targetDir, filename);
            File tempCompressedFile = null;
            BufferedOutputStream bos = null;

            if (compress) {
                tempCompressedFile = new File(backupDir, "temp_group_recv_" + System.currentTimeMillis() + "_" + filename + ".lz4");
                bos = new BufferedOutputStream(new FileOutputStream(tempCompressedFile, resumePosition > 0), 128 * 1024);
                WifeLogger.log(TAG, "Receiving compressed stream. Temporary target: " + tempCompressedFile.getAbsolutePath());
            } else {
                bos = new BufferedOutputStream(new FileOutputStream(fileDest, resumePosition > 0), 128 * 1024);
                WifeLogger.log(TAG, "Receiving raw stream. Direct target destination: " + fileDest.getAbsolutePath());
            }

            try {
                byte[] buffer = new byte[65536]; // Optimized 64KB block buffer for high-speed reception
                long totalBytesRead = resumePosition;
                long lastNotificationUpdateTime = System.currentTimeMillis();
                long speedPeriodBytesRead = 0;
                long speedPeriodStartTime = System.currentTimeMillis();
                double currentSpeed = 0.0;

                while (totalBytesRead < compressedSize && !GroupFileTransferForegroundService.isCancelled) {
                    synchronized (GroupFileTransferForegroundService.pauseLock) {
                        while (GroupFileTransferForegroundService.isPaused && !GroupFileTransferForegroundService.isCancelled) {
                            try {
                                GroupFileTransferForegroundService.pauseLock.wait();
                            } catch (InterruptedException ignored) {}
                        }
                    }

                    if (GroupFileTransferForegroundService.isCancelled) {
                        break;
                    }

                    int bytesToRead = (int) Math.min(buffer.length, compressedSize - totalBytesRead);
                    int read = proxyIn.read(buffer, 0, bytesToRead);
                    if (read == -1) {
                        throw new IOException("Connection severed abruptly during raw payload transfer.");
                    }

                    bos.write(buffer, 0, read);
                    totalBytesRead += read;
                    speedPeriodBytesSent += read;

                    long currentTime = System.currentTimeMillis();
                    long timeDiff = currentTime - speedPeriodStartTime;
                    if (timeDiff >= 1000) {
                        currentSpeed = ((double) speedPeriodBytesSent / (1024.0 * 1024.0)) / ((double) timeDiff / 1000.0);
                        speedPeriodBytesSent = 0;
                        speedPeriodStartTime = currentTime;
                    }

                    if (currentTime - lastNotificationUpdateTime >= 1000) {
                        int percent = (int) ((totalBytesRead * 100) / compressedSize);
                        notifyProgress(context, filename, percent, totalBytesRead, compressedSize, fileIndex, currentSpeed);
                        lastNotificationUpdateTime = currentTime;
                    }
                }
                bos.flush();
            } finally {
                if (bos != null) {
                    try { bos.close(); } catch (Exception ignored) {}
                }
            }

            if (!GroupFileTransferForegroundService.isCancelled) {
                if (compress) {
                    WifeLogger.log(TAG, "Compressed payload received. Decompressing group file locally: " + fileDest.getAbsolutePath());

                    try (FileInputStream fis = new FileInputStream(tempCompressedFile);
                         BufferedInputStream bis = new BufferedInputStream(fis, 128 * 1024);
                         FileOutputStream fos = new FileOutputStream(fileDest);
                         BufferedOutputStream bosOut = new BufferedOutputStream(fos, 128 * 1024)) {
                        CompressionUtils.decompress(bis, bosOut);
                    } finally {
                        if (tempCompressedFile.exists()) {
                            tempCompressedFile.delete();
                        }
                    }
                }

                WifeLogger.log(TAG, "Group File successfully received and saved: " + fileDest.getAbsolutePath());

                FileEntity entity = new FileEntity(filename, originalSize, fileDest.getAbsolutePath(), System.currentTimeMillis());
                RoomDatabaseManager.getInstance(context).fileDao().insert(entity);

                notifyComplete(context, filename, fileDest.getAbsolutePath(), fileIndex);
                fileIndex++;
            }
        }
    }

    private static File getTargetDirectory(Context context, String filename) {
        File rootDir;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            rootDir = new File(Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS), "wife shared");
        } else {
            rootDir = new File(Environment.getExternalStorageDirectory(), "wife shared");
        }

        String ext = "";
        int idx = filename.lastIndexOf('.');
        if (idx > 0) {
            ext = filename.substring(idx + 1).toLowerCase(Locale.US);
        }

        String subFolder;
        switch (ext) {
            case "mp3":
            case "emv":
            case "wav":
            case "ogg":
            case "m4a":
            case "aac":
                subFolder = "music";
                break;
            case "jpg":
            case "jpeg":
                subFolder = "images";
                break;
            case "mp4":
            case "mkv":
                subFolder = "videos";
                break;
            case "pdf":
            case "txt":
            case "doc":
            case "docx":
            case "xls":
            case "xlsx":
                subFolder = "document";
                break;
            default:
                subFolder = "misc";
                break;
        }

        File targetDir = new File(rootDir, subFolder);
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        return targetDir;
    }

    private static File getBackupDirectory() {
        File rootDir;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            rootDir = new File(Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS), "wife shared/backups");
        } else {
            rootDir = new File(Environment.getExternalStorageDirectory(), "wife shared/backups");
        }
        if (!rootDir.exists()) {
            rootDir.mkdirs();
        }
        return rootDir;
    }

    // --- Dynamic UI/Notification progress helpers for parallel stream receivers ---

    private static void notifyProgress(Context context, final String filename, final int percent, long transferred, long total, int fileIndex, double speed) {
        long now = System.currentTimeMillis();
        Long lastTime = lastBroadcastTimes.get(filename);
        
        boolean isComplete = (transferred >= total || percent >= 100);

        if (isComplete || lastTime == null || (now - lastTime >= 1000)) {
            lastBroadcastTimes.put(filename, now);

            new Handler(Looper.getMainLooper()).post(() -> {
                synchronized (FileReceiver.class) {
                    for (FileReceiveListener l : listeners) {
                        l.onProgress(filename, percent);
                    }
                }
            });

            Intent intent = new Intent(Constants.ACTION_GROUP_TRANSFER_PROGRESS);
            intent.putExtra(Constants.EXTRA_FILE_NAME, filename);
            intent.putExtra(Constants.EXTRA_BYTES_TRANSFERRED, transferred);
            intent.putExtra(Constants.EXTRA_TOTAL_BYTES, total);
            intent.putExtra(Constants.EXTRA_FILE_INDEX, fileIndex);
            intent.putExtra(Constants.EXTRA_TRANSFER_SPEED, speed);
            intent.putExtra("IS_CHUNK", false);
            LocalBroadcastManager.getInstance(context).sendBroadcast(intent);

            String speedText = String.format(Locale.US, "%.1f MB/s", speed);
            Intent serviceIntent = new Intent(context, GroupFileTransferForegroundService.class);
            serviceIntent.setAction("UPDATE_NOTIF");
            serviceIntent.putExtra("NOTIF_TEXT", "Group Recv: " + filename + " (" + percent + "%) - " + speedText);
            serviceIntent.putExtra("PROGRESS", percent);
            context.startService(serviceIntent);
        }
    }

    private static void notifyComplete(Context context, final String filename, final String path, int fileIndex) {
        new Handler(Looper.getMainLooper()).post(() -> {
            synchronized (FileReceiver.class) {
                for (FileReceiveListener l : listeners) {
                    l.onComplete(filename, path);
                }
            }
        });

        Intent intent = new Intent(Constants.ACTION_GROUP_TRANSFER_PROGRESS);
        intent.putExtra(Constants.EXTRA_FILE_NAME, filename);
        intent.putExtra(Constants.EXTRA_BYTES_TRANSFERRED, 1L); 
        intent.putExtra(Constants.EXTRA_TOTAL_BYTES, 1L);
        intent.putExtra(Constants.EXTRA_FILE_INDEX, fileIndex);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);
    }

    private static void broadcastCompletion(Context context) {
        Intent intent = new Intent(Constants.ACTION_GROUP_TRANSFER_COMPLETE);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);
    }

    private static void broadcastError(Context context, String message) {
        Intent intent = new Intent(Constants.ACTION_GROUP_TRANSFER_ERROR);
        intent.putExtra(Constants.EXTRA_ERROR_MESSAGE, message);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);
    }

    private static void notifyError(final String error) {
        new Handler(Looper.getMainLooper()).post(() -> {
            synchronized (FileReceiver.class) {
                for (FileReceiveListener l : listeners) {
                    l.onError(error);
                }
            }
        });
    }

    private static class NonClosingInputStream extends InputStream {
        private final InputStream delegate;

        public NonClosingInputStream(InputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            Log.d(TAG, "Intercepted close() request. Stream remains open.");
        }
    }
}