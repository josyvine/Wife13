package com.wife.app;

import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.Environment;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;

import androidx.localbroadcastmanager.content.LocalBroadcastManager;

import com.google.gson.JsonObject;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupFileSender {
    private static final String TAG = "GroupFileSender";
    private static volatile GroupFileSender instance;

    private final Context context;
    private final ExecutorService executorService;
    private final Handler mainHandler;

    public static GroupFileSender getInstance(Context context) {
        if (instance == null) {
            synchronized (GroupFileSender.class) {
                if (instance == null) {
                    instance = new GroupFileSender(context.getApplicationContext());
                }
            }
        }
        return instance;
    }

    private GroupFileSender(Context context) {
        this.context = context;
        // High-speed parallel sender threads to handle concurrent multi-unicast transfers to up to 5 target IPs
        this.executorService = Executors.newFixedThreadPool(5);
        this.mainHandler = new Handler(Looper.getMainLooper());
    }

    /**
     * Primary entry point for parallel multi-device file transfers.
     * Launches concurrent execution workers, one for each selected target peer IP.
     */
    public void sendGroupQueue(final List<Uri> uris, final List<String> fileNames, final long[] fileSizes, final List<String> targetIps) {
        if (uris == null || uris.isEmpty() || targetIps == null || targetIps.isEmpty()) {
            WifeLogger.log(TAG, "sendGroupQueue() aborted: Empty file queue or missing target IPs.");
            return;
        }

        WifeLogger.log(TAG, "sendGroupQueue() initiated. Files count: " + uris.size() + " | Targets count: " + targetIps.size());

        // Increment the active transactions counter inside the Group Foreground Service
        GroupFileTransferForegroundService.activeTransfersCount.addAndGet(targetIps.size());

        GroupFileTransferForegroundService.isCancelled = false;
        GroupFileTransferForegroundService.isPaused = false;

        // Dispatch a parallel sending thread for each selected target IP address
        for (final String peerIp : targetIps) {
            executorService.execute(() -> {
                SocketChannel socketChannel = null;
                try {
                    WifeLogger.log(TAG, "Initiating parallel TCP socket connection to: " + peerIp + " on Port " + Constants.OFF_PORT_GROUP_FILE);
                    
                    socketChannel = SocketChannel.open();
                    socketChannel.socket().setTcpNoDelay(true);
                    socketChannel.socket().setSendBufferSize(1024 * 1024); // 1MB buffer for 5 GHz high-speed transfers
                    socketChannel.connect(new InetSocketAddress(peerIp, Constants.OFF_PORT_GROUP_FILE));
                    socketChannel.configureBlocking(true);

                    OutputStream socketOs = socketChannel.socket().getOutputStream();

                    for (int i = 0; i < uris.size(); i++) {
                        if (GroupFileTransferForegroundService.isCancelled) {
                            WifeLogger.log(TAG, "Group transfer queue canceled by user. Terminating sender thread for IP: " + peerIp);
                            break;
                        }

                        Uri fileUri = uris.get(i);
                        String fileName = fileNames.get(i);
                        long fileSize = fileSizes[i];

                        WifeLogger.log(TAG, "Streaming file [" + fileName + "] to IP [" + peerIp + "]. Progress: [" + (i + 1) + "/" + uris.size() + "]");
                        sendSequentialFilePersistent(fileUri, fileName, fileSize, socketOs, i, peerIp);
                    }

                    // Write the end of stream marker (metadata length of 0) to notify recipient cleanly
                    if (socketChannel.isConnected()) {
                        byte[] endMarker = new byte[4]; // 0 metadata length
                        socketOs.write(endMarker);
                        socketOs.flush();
                    }

                    WifeLogger.log(TAG, "Parallel group file transmission finalized successfully for IP: " + peerIp);
                    broadcastCompletion(peerIp);

                } catch (Exception e) {
                    WifeLogger.log(TAG, "Group file sending task failed for IP: " + peerIp + " | Error: " + e.getMessage(), e);
                    broadcastError(peerIp, e.getMessage());
                } finally {
                    if (socketChannel != null) {
                        try {
                            socketChannel.socket().shutdownOutput();
                        } catch (Exception ignored) {}
                        try {
                            socketChannel.close();
                        } catch (IOException ignored) {}
                    }
                    // Safely decrement transactions and handle foreground service lifecycle
                    GroupFileTransferForegroundService.decrementAndCheckStop(context);
                }
            });
        }
    }

    private void sendSequentialFilePersistent(Uri fileUri, String fileName, long fileSize, OutputStream socketOs, int fileIndex, String peerIp) throws Exception {
        boolean compress = !isPreCompressed(fileName);
        long compressedSize = fileSize;
        File tempCompressedFile = null;

        try {
            if (compress) {
                tempCompressedFile = new File(getBackupDirectory(), "temp_send_group_" + UUID.randomUUID().toString() + "_" + fileName + ".lz4");
                try (InputStream is = context.getContentResolver().openInputStream(fileUri);
                     BufferedInputStream bis = new BufferedInputStream(is, 128 * 1024);
                     FileOutputStream fos = new FileOutputStream(tempCompressedFile);
                     BufferedOutputStream bos = new BufferedOutputStream(fos, 128 * 1024);
                     net.jpountz.lz4.LZ4FrameOutputStream lz4Out = new net.jpountz.lz4.LZ4FrameOutputStream(bos, net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE.SIZE_256KB)) {

                    if (is == null) throw new IOException("Failed opening content URI stream descriptor.");

                    byte[] buffer = new byte[65536];
                    int read;
                    long bytesReadTotal = 0;
                    long lastProgressUpdate = System.currentTimeMillis();

                    while ((read = bis.read(buffer)) != -1) {
                        if (GroupFileTransferForegroundService.isCancelled) break;

                        synchronized (GroupFileTransferForegroundService.pauseLock) {
                            while (GroupFileTransferForegroundService.isPaused && !GroupFileTransferForegroundService.isCancelled) {
                                try {
                                    GroupFileTransferForegroundService.pauseLock.wait();
                                } catch (InterruptedException ignored) {}
                            }
                        }

                        lz4Out.write(buffer, 0, read);
                        bytesReadTotal += read;

                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastProgressUpdate >= 1000) {
                            int percent = (fileSize > 0) ? (int) ((bytesReadTotal * 100) / fileSize) : 0;
                            broadcastProgress("Compressing: " + fileName, bytesReadTotal, fileSize, percent, fileIndex, 0.0, peerIp);
                            lastProgressUpdate = currentTime;
                        }
                    }
                    lz4Out.flush();
                }
                compressedSize = tempCompressedFile.length();
                WifeLogger.log(TAG, "Compression complete for group file: " + fileName + " | Original: " + fileSize + " | Compressed: " + compressedSize);
            }

            if (GroupFileTransferForegroundService.isCancelled) return;

            JsonObject fileMeta = new JsonObject();
            fileMeta.addProperty("type", "file");
            fileMeta.addProperty("name", fileName);
            fileMeta.addProperty("size", fileSize);
            fileMeta.addProperty("compressed", compress);
            fileMeta.addProperty("compressedSize", compressedSize);
            fileMeta.addProperty("lastPosition", GroupFileTransferForegroundService.lastPosition);

            byte[] metaBytes = fileMeta.toString().getBytes(StandardCharsets.UTF_8);
            byte[] lenBytes = new byte[4];
            lenBytes[0] = (byte) ((metaBytes.length >> 24) & 0xFF);
            lenBytes[1] = (byte) ((metaBytes.length >> 16) & 0xFF);
            lenBytes[2] = (byte) ((metaBytes.length >> 8) & 0xFF);
            lenBytes[3] = (byte) (metaBytes.length & 0xFF);

            socketOs.write(lenBytes);
            socketOs.write(metaBytes);
            socketOs.flush();

            if (compress) {
                try (FileInputStream fisCompressed = new FileInputStream(tempCompressedFile);
                     BufferedInputStream bisCompressed = new BufferedInputStream(fisCompressed, 128 * 1024)) {
                    transferStreamData(bisCompressed, socketOs, compressedSize, fileName, fileIndex, peerIp);
                }
            } else {
                try (InputStream is = context.getContentResolver().openInputStream(fileUri);
                     BufferedInputStream bisRaw = new BufferedInputStream(is, 128 * 1024)) {
                    transferStreamData(bisRaw, socketOs, fileSize, fileName, fileIndex, peerIp);
                }
            }

            if (!GroupFileTransferForegroundService.isCancelled) {
                FileEntity entity = new FileEntity(fileName, fileSize, fileUri.toString(), System.currentTimeMillis());
                RoomDatabaseManager.getInstance(context).fileDao().insert(entity);
                GroupFileTransferForegroundService.lastPosition = 0;
                broadcastProgress(fileName, compressedSize, compressedSize, 100, fileIndex, 0.0, peerIp);
            }
        } finally {
            if (tempCompressedFile != null && tempCompressedFile.exists()) {
                tempCompressedFile.delete();
            }
        }
    }

    private void transferStreamData(InputStream bis, OutputStream socketOs, long totalSize, String fileName, int fileIndex, String peerIp) throws Exception {
        byte[] buffer = new byte[65536]; // Optimized 64KB chunk buffer for high-speed network delivery
        int readBytes;
        long totalBytesSent = GroupFileTransferForegroundService.lastPosition;
        long lastNotificationUpdateTime = System.currentTimeMillis();
        long speedPeriodBytesSent = 0;
        long speedPeriodStartTime = System.currentTimeMillis();
        double currentSpeed = 0.0;

        if (totalBytesSent > 0) {
            long skipped = bis.skip(totalBytesSent);
            WifeLogger.log(TAG, "Skipped bytes successfully on resumed group stream: " + skipped);
        }

        while ((readBytes = bis.read(buffer)) != -1) {
            if (GroupFileTransferForegroundService.isCancelled) break;

            synchronized (GroupFileTransferForegroundService.pauseLock) {
                while (GroupFileTransferForegroundService.isPaused && !GroupFileTransferForegroundService.isCancelled) {
                    try {
                        GroupFileTransferForegroundService.pauseLock.wait();
                    } catch (InterruptedException ignored) {}
                }
            }

            socketOs.write(buffer, 0, readBytes);
            totalBytesSent += readBytes;
            speedPeriodBytesSent += readBytes;
            GroupFileTransferForegroundService.lastPosition = totalBytesSent;

            long currentTime = System.currentTimeMillis();
            long timeDiff = currentTime - speedPeriodStartTime;
            if (timeDiff >= 1000) {
                currentSpeed = ((double) speedPeriodBytesSent / (1024.0 * 1024.0)) / ((double) timeDiff / 1000.0);
                speedPeriodBytesSent = 0;
                speedPeriodStartTime = currentTime;
            }

            if (currentTime - lastNotificationUpdateTime >= 1000) {
                int percent = (int) ((totalBytesSent * 100) / totalSize);
                broadcastProgress(fileName, totalBytesSent, totalSize, percent, fileIndex, currentSpeed, peerIp);
                lastNotificationUpdateTime = currentTime;
            }
        }
        socketOs.flush();
    }

    private static boolean isPreCompressed(String filename) {
        if (filename == null) return false;
        String lower = filename.toLowerCase(Locale.US);
        return lower.endsWith(".mp4") || lower.endsWith(".mkv") || lower.endsWith(".avi") ||
               lower.endsWith(".mov") || lower.endsWith(".3gp") || lower.endsWith(".webm") ||
               lower.endsWith(".zip") || lower.endsWith(".rar") || lower.endsWith(".tar") ||
               lower.endsWith(".gz")  || lower.endsWith(".7z")  || lower.endsWith(".jpg")  ||
               lower.endsWith(".jpeg") || lower.endsWith(".png") || lower.endsWith(".webp") ||
               lower.endsWith(".gif") || lower.endsWith(".mp3") || lower.endsWith(".wav") ||
               lower.endsWith(".ogg") || lower.endsWith(".m4a") || lower.endsWith(".aac") ||
               lower.endsWith(".pdf");
    }

    private File getBackupDirectory() {
        File rootDir;
        if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.R) {
            rootDir = new File(Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS), "wife shared/backups");
        } else {
            rootDir = new File(Environment.getExternalStorageDirectory(), "wife shared/backups");
        }
        if (!rootDir.exists()) {
            rootDir.mkdirs();
        }
        return rootDir;
    }

    private void broadcastProgress(String fileName, long transferred, long total, int percent, int fileIndex, double speed, String peerIp) {
        Intent intent = new Intent(Constants.ACTION_GROUP_TRANSFER_PROGRESS);
        intent.putExtra(Constants.EXTRA_FILE_NAME, fileName);
        intent.putExtra(Constants.EXTRA_BYTES_TRANSFERRED, transferred);
        intent.putExtra(Constants.EXTRA_TOTAL_BYTES, total);
        intent.putExtra(Constants.EXTRA_FILE_INDEX, fileIndex);
        intent.putExtra(Constants.EXTRA_TRANSFER_SPEED, speed);
        intent.putExtra(Constants.EXTRA_PEER_IP, peerIp); // Map progression strictly to the target peer's list slot
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);

        String speedText = String.format(Locale.US, "%.1f MB/s", speed);
        Intent serviceIntent = new Intent(context, GroupFileTransferForegroundService.class);
        serviceIntent.setAction("UPDATE_NOTIF");
        serviceIntent.putExtra("NOTIF_TEXT", "Group Sending: " + fileName + " (" + percent + "%) - " + speedText);
        serviceIntent.putExtra("PROGRESS", percent);
        context.startService(serviceIntent);
    }

    private void broadcastCompletion(String peerIp) {
        Intent intent = new Intent(Constants.ACTION_GROUP_TRANSFER_COMPLETE);
        intent.putExtra(Constants.EXTRA_PEER_IP, peerIp);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);
    }

    private void broadcastError(String peerIp, String message) {
        Intent intent = new Intent(Constants.ACTION_GROUP_TRANSFER_ERROR);
        intent.putExtra(Constants.EXTRA_PEER_IP, peerIp);
        intent.putExtra(Constants.EXTRA_ERROR_MESSAGE, message);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);
    }
}