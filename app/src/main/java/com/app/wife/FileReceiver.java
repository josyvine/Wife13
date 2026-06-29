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
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class FileReceiver implements Runnable {
    private static final String TAG = "FileReceiver";

    private final Context context;
    private final Socket socket;
    private final Handler mainHandler;

    public interface FileReceiveListener {
        void onProgress(String filename, int percent);
        void onComplete(String filename, String localPath);
        void onError(String error);
    }

    private static final List<FileReceiveListener> listeners = new ArrayList<>();
    
    // Thread-safe map tracking completed chunks per active transaction
    private static final ConcurrentHashMap<String, AtomicInteger> activeTransfers = new ConcurrentHashMap<>();

    // Thread-safe map tracking global last progress update timestamps per file to prevent multi-threaded IPC floods
    private static final ConcurrentHashMap<String, Long> lastBroadcastTimes = new ConcurrentHashMap<>();

    // Managed fixed-size thread pool to execute socket receivers concurrently without system thread exhaustion
    private static final ExecutorService receiverExecutor = Executors.newFixedThreadPool(5);

    public static synchronized void registerListener(FileReceiveListener listener) {
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    public static synchronized void unregisterListener(FileReceiveListener listener) {
        listeners.remove(listener);
    }

    /**
     * Backward-compatible legacy constructor.
     */
    public FileReceiver(Context context, Socket socket) {
        this.context = context;
        this.socket = socket;
        this.mainHandler = new Handler(Looper.getMainLooper());
    }

    /**
     * Backward-compatible thread entry point.
     */
    @Override
    public void run() {
        WifeLogger.log(TAG, "Legacy FileReceiver runnable invoked. Redirecting to single socket processor.");
        try {
            // Symmetrical increment: Track this session as an active background transaction
            FileTransferForegroundService.activeTransfersCount.incrementAndGet();

            socket.getChannel().configureBlocking(true);
            processPersistentStream(context, socket.getChannel());
        } catch (Exception e) {
            WifeLogger.log(TAG, "Error executing legacy fallback: " + e.getMessage(), e);
            notifyError(e.getMessage());
        } finally {
            // FIXED: Centralized decrement handles safe teardown instead of blind service termination
            FileTransferForegroundService.decrementAndCheckStop(context);
        }
    }

    /**
     * Symmetrical server socket entry point called directly by the active foreground service.
     */
    public static void startServer(final Context context) {
        new Thread(() -> {
            ServerSocketChannel serverChannel = null;
            try {
                WifeLogger.log(TAG, "Opening ServerSocketChannel on port: " + Constants.OFF_PORT_FILE);
                serverChannel = ServerSocketChannel.open();
                serverChannel.socket().bind(new InetSocketAddress(Constants.OFF_PORT_FILE));
                WifeLogger.log(TAG, "ServerSocketChannel successfully bound. Entering persistent accept loop.");

                while (serverChannel.isOpen()) {
                    SocketChannel clientChannel = null;
                    try {
                        clientChannel = serverChannel.accept();
                        clientChannel.configureBlocking(true);

                        // High-Speed Socket Configurations on accepted client socket
                        clientChannel.socket().setTcpNoDelay(true);
                        clientChannel.socket().setReceiveBufferSize(1024 * 1024); // 1MB input buffer

                        final SocketChannel finalChannel = clientChannel;
                        
                        // Hand off accepted connection to the executor pool to support up to 5 parallel workers safely
                        receiverExecutor.execute(() -> {
                            try {
                                String clientIp = finalChannel.socket().getInetAddress().getHostAddress();
                                WifeLogger.log(TAG, "Processing parallel transfer stream connection from: " + clientIp);
                                
                                // Symmetrical increment: Track this newly accepted socket connection as an active transaction
                                FileTransferForegroundService.activeTransfersCount.incrementAndGet();

                                processPersistentStream(context, finalChannel);
                            } catch (Exception e) {
                                WifeLogger.log(TAG, "Parallel socket stream connection failed: " + e.getMessage(), e);
                                broadcastError(context, e.getMessage());
                            } finally {
                                try {
                                    finalChannel.close();
                                } catch (IOException ignored) {}
                                
                                // FIXED: Centralized decrement handles safe teardown instead of blind service termination
                                FileTransferForegroundService.decrementAndCheckStop(context);
                            }
                        });

                    } catch (Exception e) {
                        WifeLogger.log(TAG, "Active socket connection accept failed: " + e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                WifeLogger.log(TAG, "ServerSocketChannel threw exception or was closed: " + e.getMessage());
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
     * Persistent Multi-File Stream Processor.
     * Processes metadata headers and LZ4 decompression segments sequentially over the active SocketChannel.
     */
    private static void processPersistentStream(Context context, SocketChannel socketChannel) throws Exception {
        FileTransferForegroundService.isCancelled = false;
        FileTransferForegroundService.isPaused = false;

        InputStream rawSocketIn = socketChannel.socket().getInputStream();
        NonClosingInputStream proxyIn = new NonClosingInputStream(rawSocketIn);
        int fileIndex = 0;

        while (!FileTransferForegroundService.isCancelled && socketChannel.isConnected()) {
            byte[] lenBytes = new byte[4];
            int bytesRead = 0;
            while (bytesRead < 4 && !FileTransferForegroundService.isCancelled) {
                synchronized (FileTransferForegroundService.pauseLock) {
                    while (FileTransferForegroundService.isPaused && !FileTransferForegroundService.isCancelled) {
                        try {
                            WifeLogger.log(TAG, "Receiver thread entering wait state due to active pause command.");
                            FileTransferForegroundService.pauseLock.wait();
                        } catch (InterruptedException e) {
                            WifeLogger.log(TAG, "Receiver pause monitor thread interrupted.");
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                if (FileTransferForegroundService.isCancelled) {
                    break;
                }

                int read = proxyIn.read(lenBytes, bytesRead, 4 - bytesRead);
                if (read == -1) {
                    // Symmetrical solution for single/multi file sequential queues:
                    // If stream ends gracefully on file boundary, shut down UI & Service cleanly (Glitch Fix)
                    if (bytesRead == 0) {
                        WifeLogger.log(TAG, "Persistent stream disconnected cleanly by sender. Closing queue.");
                        broadcastCompletion(context);
                    } else {
                        WifeLogger.log(TAG, "Stream ended abruptly while reading metadata length.");
                    }
                    return;
                }
                bytesRead += read;
            }

            if (FileTransferForegroundService.isCancelled) {
                break;
            }

            int metadataLength = ((lenBytes[0] & 0xFF) << 24) |
                                 ((lenBytes[1] & 0xFF) << 16) |
                                 ((lenBytes[2] & 0xFF) << 8)  |
                                 (lenBytes[3] & 0xFF);

            if (metadataLength == 0) {
                WifeLogger.log(TAG, "End of persistent queue stream marker received. Closing stream.");
                broadcastCompletion(context);
                break;
            }

            byte[] metaBytes = new byte[metadataLength];
            bytesRead = 0;
            while (bytesRead < metadataLength && !FileTransferForegroundService.isCancelled) {
                synchronized (FileTransferForegroundService.pauseLock) {
                    while (FileTransferForegroundService.isPaused && !FileTransferForegroundService.isCancelled) {
                        try {
                            WifeLogger.log(TAG, "Receiver thread entering wait state due to active pause command.");
                            FileTransferForegroundService.pauseLock.wait();
                        } catch (InterruptedException e) {
                            WifeLogger.log(TAG, "Receiver pause monitor thread interrupted.");
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                if (FileTransferForegroundService.isCancelled) {
                    break;
                }

                int read = proxyIn.read(metaBytes, bytesRead, metadataLength - bytesRead);
                if (read == -1) {
                    throw new IOException("Stream ended abruptly while reading metadata payload.");
                }
                bytesRead += read;
            }

            if (FileTransferForegroundService.isCancelled) {
                break;
            }

            String metaJson = new String(metaBytes, StandardCharsets.UTF_8);
            JsonObject meta = JsonParser.parseString(metaJson).getAsJsonObject();

            final String filename = meta.get("name").getAsString();
            final long originalSize = meta.get("size").getAsLong();
            final long compressedSize = meta.get("compressedSize").getAsLong();
            final boolean compress = meta.has("compressed") ? meta.get("compressed").getAsBoolean() : true;
            long resumePosition = meta.has("lastPosition") ? meta.get("lastPosition").getAsLong() : 0;
            
            String transferType = meta.has("type") ? meta.get("type").getAsString() : "file";

            WifeLogger.log(TAG, "Processing incoming payload: " + filename + " | Type: " + transferType + " | Size: " + originalSize + " bytes");

            File targetDir = getTargetDirectory(context, filename);
            File backupDir = getBackupDirectory();

            if ("chunk".equals(transferType)) {
                final String fileId = meta.get("fileId").getAsString();
                final int chunkIndex = meta.get("chunkIndex").getAsInt();
                final int totalChunks = meta.get("totalChunks").getAsInt();

                WifeLogger.log(TAG, "Processing parallel chunk [" + chunkIndex + "/" + totalChunks + "] for File ID: " + fileId);

                File tempCompressedChunk = new File(backupDir, "chunk_" + fileId + "_" + chunkIndex + ".lz4");
                File tempRawChunk = new File(backupDir, "chunk_" + fileId + "_" + chunkIndex + ".raw");

                try {
                    try (FileOutputStream fos = new FileOutputStream(tempCompressedChunk);
                         BufferedOutputStream bos = new BufferedOutputStream(fos, 128 * 1024)) {
                        byte[] buffer = new byte[65536]; // Increased from 16KB to 64KB for high-speed reception
                        long totalBytesRead = 0;
                        long lastNotifTime = System.currentTimeMillis();
                        while (totalBytesRead < compressedSize && !FileTransferForegroundService.isCancelled) {
                            int bytesToRead = (int) Math.min(buffer.length, compressedSize - totalBytesRead);
                            int read = proxyIn.read(buffer, 0, bytesToRead);
                            if (read == -1) {
                                throw new IOException("Connection severed abruptly during raw chunk payload transfer.");
                            }
                            bos.write(buffer, 0, read);
                            totalBytesRead += read;

                            long now = System.currentTimeMillis();
                            if (now - lastNotifTime >= 1000) {
                                // Feed chunk progress updates relative to compressed segment size during transaction
                                int globalCompleted = activeTransfers.getOrDefault(fileId, new AtomicInteger(0)).get();
                                int globalPercent = (globalCompleted * 100) / totalChunks;
                                notifyChunkProgress(context, filename, globalPercent, globalCompleted * 20L * 1024L * 1024L + totalBytesRead, originalSize, fileIndex, 0.0, chunkIndex, totalBytesRead, compressedSize);
                                lastNotifTime = now;
                            }
                        }
                        bos.flush();
                    }

                    if (!FileTransferForegroundService.isCancelled) {
                        try (FileInputStream fis = new FileInputStream(tempCompressedChunk);
                             BufferedInputStream bis = new BufferedInputStream(fis, 128 * 1024);
                             FileOutputStream fos = new FileOutputStream(tempRawChunk);
                             BufferedOutputStream bos = new BufferedOutputStream(fos, 128 * 1024)) {
                            CompressionUtils.decompress(bis, bos);
                        }

                        activeTransfers.putIfAbsent(fileId, new AtomicInteger(0));
                        int completed = activeTransfers.get(fileId).incrementAndGet();

                        int percent = (completed * 100) / totalChunks;
                        notifyChunkProgress(context, filename, percent, completed * 20L * 1024L * 1024L, originalSize, fileIndex, 0.0, chunkIndex, compressedSize, compressedSize);

                        if (completed == totalChunks) {
                            // FIXED: Force immediate shutdown and closure of current socket connection
                            // to ensure FIN/ACK handshakes complete before the eMMC merging locks the system.
                            try {
                                socketChannel.socket().shutdownInput();
                                socketChannel.socket().shutdownOutput();
                            } catch (Exception ignored) {}
                            try {
                                socketChannel.close();
                            } catch (Exception ignored) {}

                            mergeChunksAndFinalize(context, fileId, totalChunks, targetDir, filename, originalSize, fileIndex);
                        }
                    }
                } finally {
                    if (tempCompressedChunk.exists()) {
                        tempCompressedChunk.delete();
                    }
                }
                break;

            } else {
                File fileDest = new File(targetDir, filename);
                File tempCompressedFile = null;
                BufferedOutputStream bos = null;

                // Open write endpoint directly on target location if file is uncompressed
                if (compress) {
                    tempCompressedFile = new File(backupDir, "temp_recv_" + System.currentTimeMillis() + "_" + filename + ".lz4");
                    bos = new BufferedOutputStream(new FileOutputStream(tempCompressedFile, resumePosition > 0), 128 * 1024);
                    WifeLogger.log(TAG, "Receiving compressed stream. Temporary target: " + tempCompressedFile.getAbsolutePath());
                } else {
                    bos = new BufferedOutputStream(new FileOutputStream(fileDest, resumePosition > 0), 128 * 1024);
                    WifeLogger.log(TAG, "Receiving raw stream. Direct target destination: " + fileDest.getAbsolutePath());
                }

                try {
                    byte[] buffer = new byte[65536]; // Increased from 16KB to 64KB for high-speed reception
                    long totalBytesRead = resumePosition;
                    long lastNotificationUpdateTime = System.currentTimeMillis();
                    long speedPeriodBytesRead = 0;
                    long speedPeriodStartTime = System.currentTimeMillis();
                    double currentSpeed = 0.0;

                    while (totalBytesRead < compressedSize && !FileTransferForegroundService.isCancelled) {
                        synchronized (FileTransferForegroundService.pauseLock) {
                            while (FileTransferForegroundService.isPaused && !FileTransferForegroundService.isCancelled) {
                                try {
                                    FileTransferForegroundService.pauseLock.wait();
                                } catch (InterruptedException ignored) {}
                            }
                        }

                        if (FileTransferForegroundService.isCancelled) {
                            break;
                        }

                        int bytesToRead = (int) Math.min(buffer.length, compressedSize - totalBytesRead);
                        int read = proxyIn.read(buffer, 0, bytesToRead);
                        if (read == -1) {
                            throw new IOException("Connection severed abruptly during raw payload transfer.");
                        }

                        bos.write(buffer, 0, read);
                        totalBytesRead += read;
                        speedPeriodBytesRead += read;

                        long currentTime = System.currentTimeMillis();
                        long timeDiff = currentTime - speedPeriodStartTime;
                        if (timeDiff >= 1000) {
                            currentSpeed = ((double) speedPeriodBytesRead / (1024.0 * 1024.0)) / ((double) timeDiff / 1000.0);
                            speedPeriodBytesRead = 0;
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

                if (!FileTransferForegroundService.isCancelled) {
                    if (compress) {
                        WifeLogger.log(TAG, "Compressed payload received. Decompressing file locally: " + fileDest.getAbsolutePath());

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

                    WifeLogger.log(TAG, "File successfully received and saved: " + fileDest.getAbsolutePath());

                    FileEntity entity = new FileEntity(filename, originalSize, fileDest.getAbsolutePath(), System.currentTimeMillis());
                    RoomDatabaseManager.getInstance(context).fileDao().insert(entity);

                    notifyComplete(context, filename, fileDest.getAbsolutePath(), fileIndex);
                    fileIndex++;
                }
            }
        }
    }

    /**
     * Glues raw temporary chunk files back into a single valid uncompressed file via the high-speed FileJoiner.
     */
    private static void mergeChunksAndFinalize(Context context, String fileId, int totalChunks, File targetDir, String filename, long originalSize, int fileIndex) {
        // FIXED: Increment the active transactions counter before spawning the background merge thread to prevent race condition teardown
        FileTransferForegroundService.activeTransfersCount.incrementAndGet();

        new Thread(() -> {
            try {
                File fileDest = new File(targetDir, filename);
                WifeLogger.log(TAG, "All chunks received. Merging chunk parts into final file: " + fileDest.getAbsolutePath());

                boolean success = FileJoiner.mergeParts(context, fileId, totalChunks, fileDest);

                if (success) {
                    try {
                        WifeLogger.log(TAG, "File successfully decrypted, merged and saved: " + fileDest.getAbsolutePath());

                        FileEntity entity = new FileEntity(filename, originalSize, fileDest.getAbsolutePath(), System.currentTimeMillis());
                        RoomDatabaseManager.getInstance(context).fileDao().insert(entity);

                        notifyComplete(context, filename, fileDest.getAbsolutePath(), fileIndex);
                        
                        // Symmetrical solution for parallel chunked files:
                        // Force complete system tray notification and layout dismissal on parallel merge completion (Glitch Fix)
                        broadcastCompletion(context);
                        
                        activeTransfers.remove(fileId);
                        lastBroadcastTimes.remove(filename);
                    } catch (Exception e) {
                        WifeLogger.log(TAG, "Database log persistence failed after zero-copy merge: " + e.getMessage(), e);
                        broadcastError(context, e.getMessage());
                    }
                } else {
                    WifeLogger.log(TAG, "Zero-copy chunk merge operation failed for File ID: " + fileId);
                    broadcastError(context, "Assembling of the chunked payload failed on receiver side.");
                }
            } finally {
                // FIXED: Centralized decrement handles safe teardown inside finally block once the asynchronous merge thread completes
                FileTransferForegroundService.decrementAndCheckStop(context);
            }
        }).start();
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

    // --- UI/Notification Broadcast dispatchers ---

    private static void notifyProgress(Context context, final String filename, final int percent, long transferred, long total, int fileIndex, double speed) {
        long now = System.currentTimeMillis();
        Long lastTime = lastBroadcastTimes.get(filename);
        
        // FIXED: Force-bypass the 1-second progress rate limit if the file is completely received (percent >= 100)
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

            Intent intent = new Intent(Constants.ACTION_TRANSFER_PROGRESS);
            intent.putExtra(Constants.EXTRA_FILE_NAME, filename);
            intent.putExtra(Constants.EXTRA_BYTES_TRANSFERRED, transferred);
            intent.putExtra(Constants.EXTRA_TOTAL_BYTES, total);
            intent.putExtra(Constants.EXTRA_FILE_INDEX, fileIndex);
            intent.putExtra(Constants.EXTRA_TRANSFER_SPEED, speed);
            intent.putExtra("IS_CHUNK", false);
            LocalBroadcastManager.getInstance(context).sendBroadcast(intent);

            String speedText = String.format(Locale.US, "%.1f MB/s", speed);
            Intent serviceIntent = new Intent(context, FileTransferForegroundService.class);
            serviceIntent.setAction("UPDATE_NOTIF");
            serviceIntent.putExtra("NOTIF_TEXT", "Receiving " + filename + " (" + percent + "%) - " + speedText);
            serviceIntent.putExtra("PROGRESS", percent);
            context.startService(serviceIntent);
        }
    }

    private static void notifyChunkProgress(Context context, final String filename, final int percent, long transferred, long total, int fileIndex, double speed, int chunkIndex, long chunkTransferred, long chunkTotal) {
        long now = System.currentTimeMillis();
        Long lastTime = lastBroadcastTimes.get(filename);

        // FIXED: Force-bypass the 1-second progress rate limit if the chunk itself is completely received (chunkTransferred >= chunkTotal)
        boolean isChunkComplete = (chunkTransferred >= chunkTotal);
        boolean isFileComplete = (percent >= 100);

        if (isChunkComplete || isFileComplete || lastTime == null || (now - lastTime >= 1000)) {
            lastBroadcastTimes.put(filename, now);

            new Handler(Looper.getMainLooper()).post(() -> {
                synchronized (FileReceiver.class) {
                    for (FileReceiveListener l : listeners) {
                        l.onProgress(filename, percent);
                    }
                }
            });

            Intent intent = new Intent(Constants.ACTION_TRANSFER_PROGRESS);
            intent.putExtra(Constants.EXTRA_FILE_NAME, filename);
            intent.putExtra(Constants.EXTRA_BYTES_TRANSFERRED, transferred);
            intent.putExtra(Constants.EXTRA_TOTAL_BYTES, total);
            intent.putExtra(Constants.EXTRA_FILE_INDEX, fileIndex);
            intent.putExtra(Constants.EXTRA_TRANSFER_SPEED, speed);
            intent.putExtra("IS_CHUNK", true);
            intent.putExtra("CHUNK_INDEX", chunkIndex);
            intent.putExtra("CHUNK_BYTES_TRANSFERRED", chunkTransferred);
            intent.putExtra("CHUNK_TOTAL_BYTES", chunkTotal);
            LocalBroadcastManager.getInstance(context).sendBroadcast(intent);

            String speedText = String.format(Locale.US, "%.1f MB/s", speed);
            Intent serviceIntent = new Intent(context, FileTransferForegroundService.class);
            serviceIntent.setAction("UPDATE_NOTIF");
            serviceIntent.putExtra("NOTIF_TEXT", "Receiving Chunk #" + (chunkIndex + 1) + " of " + filename + " (" + percent + "%) - " + speedText);
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

        Intent intent = new Intent(Constants.ACTION_TRANSFER_PROGRESS);
        intent.putExtra(Constants.EXTRA_FILE_NAME, filename);
        intent.putExtra(Constants.EXTRA_BYTES_TRANSFERRED, 1L); 
        intent.putExtra(Constants.EXTRA_TOTAL_BYTES, 1L);
        intent.putExtra(Constants.EXTRA_FILE_INDEX, fileIndex);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);
    }

    private static void broadcastCompletion(Context context) {
        Intent intent = new Intent(Constants.ACTION_TRANSFER_COMPLETE);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);
        
        // FIXED: Blind stopService invocation removed. Service lifecycle is now managed by decrementAndCheckStop.
    }

    private static void broadcastError(Context context, String message) {
        Intent intent = new Intent(Constants.ACTION_TRANSFER_ERROR);
        intent.putExtra(Constants.EXTRA_ERROR_MESSAGE, message);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);

        // FIXED: Blind stopService invocation removed. Service lifecycle is now managed by decrementAndCheckStop.
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