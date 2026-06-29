package com.wife.app;

import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.Build;
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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FileSender {
    private static final String TAG = "FileSender";
    private static volatile FileSender instance;

    // Raised threshold to 10 GB to route all transfers sequentially through our optimized, zero-overhead pipeline
    private static final long CHUNK_THRESHOLD = 10L * 1024L * 1024L * 1024L; 
    private static final int CHUNK_SIZE = 20 * 1024 * 1024;            // 20 MB Chunk Size

    private final Context context;
    private final ExecutorService executorService;
    private final Handler mainHandler;

    public interface FileTransferListener {
        void onProgress(int percent);
        void onComplete(String path);
        void onError(String error);
    }

    public static FileSender getInstance(Context context) {
        if (instance == null) {
            synchronized (FileSender.class) {
                if (instance == null) {
                    instance = new FileSender(context.getApplicationContext());
                }
            }
        }
        return instance;
    }

    private FileSender(Context context) {
        this.context = context;
        this.executorService = Executors.newSingleThreadExecutor();
        this.mainHandler = new Handler(Looper.getMainLooper());
    }

    /**
     * Backward-compatible single-file transmitter.
     * Delegates internally to the new persistent streaming queue engine.
     */
    public void sendFile(final Uri fileUri, final String originalFileName, final long fileSize, final FileTransferListener listener) {
        ArrayList<Uri> uris = new ArrayList<>(Collections.singletonList(fileUri));
        ArrayList<String> names = new ArrayList<>(Collections.singletonList(originalFileName));
        long[] sizes = new long[]{fileSize};

        final String peerIp = ConnectionManager.getInstance(context).getPeerIpAddress();
        if (peerIp == null || peerIp.isEmpty()) {
            if (listener != null) listener.onError("No connected peer available.");
            return;
        }

        WifeLogger.log(TAG, "Legacy sendFile() invoked. Wrapping in single-item queue list.");

        Intent serviceIntent = new Intent(context, FileTransferForegroundService.class);
        serviceIntent.setAction(Constants.ACTION_START_TRANSFER);
        serviceIntent.putExtra("IS_SENDER", true);
        serviceIntent.putStringArrayListExtra("URI_LIST", new ArrayList<>(Collections.singletonList(fileUri.toString())));
        serviceIntent.putStringArrayListExtra("FILE_NAMES", names);
        serviceIntent.putExtra("FILE_SIZES", sizes);
        serviceIntent.putExtra("PEER_IP", peerIp);
        context.startService(serviceIntent);
    }

    /**
     * Core Persistent High-Speed Queue Transmitter.
     * Maintains a persistent SocketChannel across sequential worker executions.
     */
    public void sendQueue(final List<Uri> uris, final List<String> fileNames, final long[] fileSizes, final String peerIp) {
        WifeLogger.log(TAG, "sendQueue() started. Files count: " + uris.size() + " | Destination Peer: " + peerIp);

        executorService.execute(() -> {
            // Symmetrical increment: Track this sending queue as an active background transaction
            FileTransferForegroundService.activeTransfersCount.incrementAndGet();

            FileTransferForegroundService.isCancelled = false;
            FileTransferForegroundService.isPaused = false;

            SocketChannel persistentChannel = null;

            try {
                for (int i = 0; i < uris.size(); i++) {
                    if (FileTransferForegroundService.isCancelled) {
                        WifeLogger.log(TAG, "Queue loop cancelled by user. Terminating sender.");
                        break;
                    }

                    Uri fileUri = uris.get(i);
                    String fileName = fileNames.get(i);
                    long fileSize = fileSizes[i];

                    if (fileSize > CHUNK_THRESHOLD) {
                        // Close any existing persistent sequential channel to avoid blocking parallel chunk sockets
                        if (persistentChannel != null) {
                            try {
                                persistentChannel.socket().shutdownOutput();
                                Thread.sleep(200);
                            } catch (Exception ignored) {}
                            try { persistentChannel.close(); } catch (Exception ignored) {}
                            persistentChannel = null;
                        }
                        WifeLogger.log(TAG, "File size exceeds threshold. Initiating parallel chunked stream pipeline.");
                        sendLargeFileInParallel(fileUri, fileName, fileSize, peerIp, i);
                    } else {
                        WifeLogger.log(TAG, "File size is below threshold. Proceeding with standard sequential LZ4 streaming.");
                        if (persistentChannel == null || !persistentChannel.isConnected() || !persistentChannel.isOpen()) {
                            persistentChannel = SocketChannel.open();
                            persistentChannel.socket().setTcpNoDelay(true);
                            persistentChannel.socket().setSendBufferSize(1024 * 1024);
                            persistentChannel.connect(new InetSocketAddress(peerIp, Constants.OFF_PORT_FILE));
                            persistentChannel.configureBlocking(true);
                        }
                        sendSequentialFilePersistent(fileUri, fileName, fileSize, persistentChannel, i);
                    }
                }

                // Send the persistent queue stream end marker (metadata length of 0) to notify receiver cleanly
                if (persistentChannel != null && persistentChannel.isConnected()) {
                    OutputStream socketOs = persistentChannel.socket().getOutputStream();
                    byte[] endMarker = new byte[4]; // 0 size
                    socketOs.write(endMarker);
                    socketOs.flush();
                }

                if (!FileTransferForegroundService.isCancelled) {
                    broadcastCompletion();
                }

            } catch (Exception e) {
                WifeLogger.log(TAG, "Persistent file sending pipeline threw fatal exception: " + e.getMessage(), e);
                broadcastError(e.getMessage());
            } finally {
                if (persistentChannel != null) {
                    try {
                        persistentChannel.socket().shutdownOutput();
                        Thread.sleep(500);
                    } catch (Exception ignored) {}
                    try { persistentChannel.close(); } catch (IOException ignored) {}
                }
                // Centralized decrement handles safe teardown instead of blind service termination
                FileTransferForegroundService.decrementAndCheckStop(context);
            }
        });
    }

    /**
     * Sends large files using parallel sockets and dynamic 20MB chunks in a throttled sliding window queue.
     * Saves files to public external storage backups to prevent ENOSPC storage exhaustion.
     */
    private void sendLargeFileInParallel(Uri fileUri, String fileName, long fileSize, String peerIp, int fileIndex) throws Exception {
        final String fileId = UUID.randomUUID().toString();
        final int totalChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);
        WifeLogger.log(TAG, "Parallel chunks metadata calculated. Total chunks to build: " + totalChunks);

        // Control loop executing chunks sequentially in paged batches of 3 to avoid I/O thrashing
        final int WINDOW_SIZE = 3;
        final List<Throwable> chunkExceptions = Collections.synchronizedList(new ArrayList<>());
        AtomicLong totalBytesSentCombined = new AtomicLong(0);

        File backupDir = getBackupDirectory();

        for (int batchStart = 0; batchStart < totalChunks; batchStart += WINDOW_SIZE) {
            if (FileTransferForegroundService.isCancelled || !chunkExceptions.isEmpty()) {
                break;
            }

            int batchEnd = Math.min(batchStart + WINDOW_SIZE, totalChunks);
            int activeBatchCount = batchEnd - batchStart;
            WifeLogger.log(TAG, "Processing Sliding Window Batch Chunks: [" + batchStart + " to " + (batchEnd - 1) + "]");

            java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(activeBatchCount);
            ExecutorService batchExecutor = Executors.newFixedThreadPool(activeBatchCount);

            for (int chunkIdx = batchStart; chunkIdx < batchEnd; chunkIdx++) {
                final int finalChunkIdx = chunkIdx;
                batchExecutor.execute(() -> {
                    boolean isChunkFullyWritten = false;
                    try {
                        if (FileTransferForegroundService.isCancelled || !chunkExceptions.isEmpty()) return;

                        File tempChunkFile = new File(backupDir, "temp_send_chunk_" + fileId + "_" + finalChunkIdx + ".lz4");
                        long startOffset = (long) finalChunkIdx * CHUNK_SIZE;
                        long rawChunkSize = Math.min(CHUNK_SIZE, fileSize - startOffset);

                        // 128KB buffer streams wrap raw file system content and output endpoints
                        try (InputStream is = context.getContentResolver().openInputStream(fileUri);
                             BufferedInputStream bis = new BufferedInputStream(is, 128 * 1024);
                             FileOutputStream fos = new FileOutputStream(tempChunkFile);
                             BufferedOutputStream bos = new BufferedOutputStream(fos, 128 * 1024);
                             net.jpountz.lz4.LZ4FrameOutputStream lz4Out = new net.jpountz.lz4.LZ4FrameOutputStream(bos, net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE.SIZE_256KB)) {

                            if (is == null) throw new IOException("Failed opening content URI stream descriptor.");

                            // Fast stream skip offset pointer progression
                            long skipped = 0;
                            while (skipped < startOffset) {
                                long skipRead = bis.skip(startOffset - skipped);
                                if (skipRead <= 0) break;
                                skipped += skipRead;
                            }

                            byte[] buffer = new byte[65536]; // Optimized 64KB block buffer
                            long bytesReadTotal = 0;
                            int read;

                            while (bytesReadTotal < rawChunkSize && (read = bis.read(buffer, 0, (int) Math.min(buffer.length, rawChunkSize - bytesReadTotal))) != -1) {
                                if (FileTransferForegroundService.isCancelled || !chunkExceptions.isEmpty()) break;

                                synchronized (FileTransferForegroundService.pauseLock) {
                                    while (FileTransferForegroundService.isPaused && !FileTransferForegroundService.isCancelled) {
                                        try {
                                            FileTransferForegroundService.pauseLock.wait();
                                        } catch (InterruptedException ignored) {}
                                    }
                                }

                                lz4Out.write(buffer, 0, read);
                                bytesReadTotal += read;
                            }
                            lz4Out.flush();
                        }

                        long compressedChunkSize = tempChunkFile.length();
                        WifeLogger.log(TAG, "Chunk [" + finalChunkIdx + "] compression finalized. Compressed: " + compressedChunkSize + " bytes.");

                        if (FileTransferForegroundService.isCancelled || !chunkExceptions.isEmpty()) return;

                        try (SocketChannel chunkChannel = SocketChannel.open()) {
                            // High-Speed Socket Configurations
                            chunkChannel.socket().setTcpNoDelay(true);
                            chunkChannel.socket().setSendBufferSize(1024 * 1024); // 1MB Output buffer
                            
                            chunkChannel.connect(new InetSocketAddress(peerIp, Constants.OFF_PORT_FILE));
                            chunkChannel.configureBlocking(true);
                            OutputStream os = chunkChannel.socket().getOutputStream();

                            JsonObject chunkMeta = new JsonObject();
                            chunkMeta.addProperty("type", "chunk");
                            chunkMeta.addProperty("fileId", fileId);
                            chunkMeta.addProperty("name", fileName);
                            chunkMeta.addProperty("chunkIndex", finalChunkIdx);
                            chunkMeta.addProperty("totalChunks", totalChunks);
                            chunkMeta.addProperty("size", fileSize);
                            chunkMeta.addProperty("compressedSize", compressedChunkSize);

                            byte[] metaBytes = chunkMeta.toString().getBytes(StandardCharsets.UTF_8);
                            byte[] lenBytes = new byte[4];
                            lenBytes[0] = (byte) ((metaBytes.length >> 24) & 0xFF);
                            lenBytes[1] = (byte) ((metaBytes.length >> 16) & 0xFF);
                            lenBytes[2] = (byte) ((metaBytes.length >> 8) & 0xFF);
                            lenBytes[3] = (byte) (metaBytes.length & 0xFF);

                            os.write(lenBytes);
                            os.write(metaBytes);
                            os.flush();

                            try (FileInputStream fis = new FileInputStream(tempChunkFile);
                                 BufferedInputStream bisFile = new BufferedInputStream(fis, 128 * 1024)) {
                                byte[] buffer = new byte[65536]; // Increased from 16KB to 64KB for faster socket I/O
                                int len;
                                long bytesSentForThisChunk = 0;
                                long speedPeriodBytesSent = 0;
                                long speedPeriodStartTime = System.currentTimeMillis();
                                double currentSpeed = 0.0;
                                long localLastNotificationTime = System.currentTimeMillis();

                                while ((len = bisFile.read(buffer)) != -1) {
                                    if (FileTransferForegroundService.isCancelled || !chunkExceptions.isEmpty()) break;

                                    synchronized (FileTransferForegroundService.pauseLock) {
                                        while (FileTransferForegroundService.isPaused && !FileTransferForegroundService.isCancelled) {
                                            try {
                                                FileTransferForegroundService.pauseLock.wait();
                                            } catch (InterruptedException ignored) {}
                                        }
                                    }

                                    os.write(buffer, 0, len);
                                    bytesSentForThisChunk += len;
                                    speedPeriodBytesSent += len;
                                    long totalSoFar = totalBytesSentCombined.addAndGet(len);

                                    long currentTime = System.currentTimeMillis();
                                    long timeDiff = currentTime - speedPeriodStartTime;
                                    if (timeDiff >= 1000) {
                                        currentSpeed = ((double) speedPeriodBytesSent / (1024.0 * 1024.0)) / ((double) timeDiff / 1000.0);
                                        speedPeriodBytesSent = 0;
                                        speedPeriodStartTime = currentTime;
                                    }

                                    long now = System.currentTimeMillis();
                                    if (now - localLastNotificationTime >= 1000) {
                                        int percent = (int) ((totalSoFar * 100) / fileSize);
                                        broadcastChunkProgress(fileName, totalSoFar, fileSize, percent, fileIndex, currentSpeed, finalChunkIdx, bytesSentForThisChunk, compressedChunkSize);
                                        localLastNotificationTime = now;
                                    }
                                }
                                os.flush();
                            }

                            isChunkFullyWritten = true; // Mark as completely written into native TCP buffers

                            int finalPercent = (int) ((totalBytesSentCombined.get() * 100) / fileSize);
                            broadcastChunkProgress(fileName, totalBytesSentCombined.get(), fileSize, finalPercent, fileIndex, 0.0, finalChunkIdx, compressedChunkSize, compressedChunkSize);

                            chunkChannel.socket().shutdownOutput();
                            Thread.sleep(500); // 500ms grace window to prevent connection truncation freezes
                        }
                    } catch (Exception e) {
                        // FIXED: Symmetrical handshake soft-failure check.
                        // If we verify that 100% of the raw bytes were successfully flushed, 
                        // any subsequent socket reset/abort during closure is safe to ignore.
                        if (isChunkFullyWritten) {
                            WifeLogger.log(TAG, "Socket reset/closed on chunk [" + finalChunkIdx + "] post-transmission during handshake. Ignoring non-fatal error.");
                        } else {
                            WifeLogger.log(TAG, "Failed parallel chunk delivery for index " + finalChunkIdx + ": " + e.getMessage());
                            chunkExceptions.add(e);
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // Sync block wait to finalize the active window batch before proceeding to subsequent tasks
            latch.await();
            batchExecutor.shutdown();
            batchExecutor.awaitTermination(5, TimeUnit.MINUTES);

            // Symmetrical cleanup: Purge physical chunk LZ4 cache files immediately after each batch is finalized
            for (int chunkIdx = batchStart; chunkIdx < batchEnd; chunkIdx++) {
                File tempChunkFile = new File(backupDir, "temp_send_chunk_" + fileId + "_" + chunkIdx + ".lz4");
                if (tempChunkFile.exists()) {
                    tempChunkFile.delete();
                }
            }
        }

        if (!chunkExceptions.isEmpty()) {
            throw new IOException("Failed parallel chunk delivery: " + chunkExceptions.get(0).getMessage());
        }

        if (FileTransferForegroundService.isCancelled) {
            throw new IOException("Parallel transfer terminated by user cancellation.");
        }

        FileEntity entity = new FileEntity(fileName, fileSize, fileUri.toString(), System.currentTimeMillis());
        RoomDatabaseManager.getInstance(context).fileDao().insert(entity);
    }

    // Helper method to identify pre-compressed media and container formats
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

    /**
     * Standard sequential LZ4 file transmitter inside the persistent SocketChannel queue.
     * Selectively bypasses compression for pre-compressed media formats to avoid wasting CPU cycles.
     */
    private void sendSequentialFilePersistent(Uri fileUri, String fileName, long fileSize, SocketChannel socketChannel, int fileIndex) throws Exception {
        OutputStream socketOs = socketChannel.socket().getOutputStream();
        
        boolean compress = !isPreCompressed(fileName);
        long compressedSize = fileSize;
        File tempCompressedFile = null;

        try {
            if (compress) {
                tempCompressedFile = new File(getBackupDirectory(), "temp_send_" + UUID.randomUUID().toString() + "_" + fileName + ".lz4");
                try (InputStream is = context.getContentResolver().openInputStream(fileUri);
                     BufferedInputStream bis = new BufferedInputStream(is, 128 * 1024);
                     FileOutputStream fos = new FileOutputStream(tempCompressedFile);
                     BufferedOutputStream bos = new BufferedOutputStream(fos, 128 * 1024);
                     net.jpountz.lz4.LZ4FrameOutputStream lz4Out = new net.jpountz.lz4.LZ4FrameOutputStream(bos, net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE.SIZE_256KB)) {

                        if (is == null) throw new IOException("Failed opening content URI stream.");

                        byte[] buffer = new byte[65536];
                        int read;
                        long bytesReadTotal = 0;
                        long lastProgressUpdate = System.currentTimeMillis();

                        while ((read = bis.read(buffer)) != -1) {
                            if (FileTransferForegroundService.isCancelled) break;

                            synchronized (FileTransferForegroundService.pauseLock) {
                                while (FileTransferForegroundService.isPaused && !FileTransferForegroundService.isCancelled) {
                                    try {
                                        FileTransferForegroundService.pauseLock.wait();
                                    } catch (InterruptedException ignored) {}
                                }
                            }

                            lz4Out.write(buffer, 0, read);
                            bytesReadTotal += read;

                            long currentTime = System.currentTimeMillis();
                            if (currentTime - lastProgressUpdate >= 1000) {
                                int percent = (fileSize > 0) ? (int) ((bytesReadTotal * 100) / fileSize) : 0;
                                broadcastProgress("Compressing: " + fileName, bytesReadTotal, fileSize, percent, fileIndex, 0.0);
                                lastProgressUpdate = currentTime;
                            }
                        }
                        lz4Out.flush();
                    }
                compressedSize = tempCompressedFile.length();
                WifeLogger.log(TAG, "Compression complete. Compressed Size: " + compressedSize + " bytes.");
            }

            if (FileTransferForegroundService.isCancelled) return;

            JsonObject fileMeta = new JsonObject();
            fileMeta.addProperty("type", "file");
            fileMeta.addProperty("name", fileName);
            fileMeta.addProperty("size", fileSize);
            fileMeta.addProperty("compressed", compress); // Transmit the compression flag to the receiver
            fileMeta.addProperty("compressedSize", compressedSize);
            fileMeta.addProperty("lastPosition", FileTransferForegroundService.lastPosition);

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
                    transferStreamData(bisCompressed, socketOs, compressedSize, fileName, fileIndex);
                }
            } else {
                try (InputStream is = context.getContentResolver().openInputStream(fileUri);
                     BufferedInputStream bisRaw = new BufferedInputStream(is, 128 * 1024)) {
                    transferStreamData(bisRaw, socketOs, fileSize, fileName, fileIndex);
                }
            }

            if (!FileTransferForegroundService.isCancelled) {
                FileEntity entity = new FileEntity(fileName, fileSize, fileUri.toString(), System.currentTimeMillis());
                RoomDatabaseManager.getInstance(context).fileDao().insert(entity);
                FileTransferForegroundService.lastPosition = 0;
                broadcastProgress(fileName, compressedSize, compressedSize, 100, fileIndex, 0.0);
            }
        } finally {
            if (tempCompressedFile != null && tempCompressedFile.exists()) {
                tempCompressedFile.delete();
            }
        }
    }

    // High-performance stream router using an optimized 64 KB network block buffer [2]
    private void transferStreamData(InputStream bis, OutputStream socketOs, long totalSize, String fileName, int fileIndex) throws Exception {
        byte[] buffer = new byte[65536];
        int readBytes;
        long totalBytesSent = FileTransferForegroundService.lastPosition;
        long lastNotificationUpdateTime = System.currentTimeMillis();
        long speedPeriodBytesSent = 0;
        long speedPeriodStartTime = System.currentTimeMillis();
        double currentSpeed = 0.0;

        if (totalBytesSent > 0) {
            long skipped = bis.skip(totalBytesSent);
            WifeLogger.log(TAG, "Skipped bytes successfully: " + skipped);
        }

        while ((readBytes = bis.read(buffer)) != -1) {
            if (FileTransferForegroundService.isCancelled) break;

            synchronized (FileTransferForegroundService.pauseLock) {
                while (FileTransferForegroundService.isPaused && !FileTransferForegroundService.isCancelled) {
                    try {
                        FileTransferForegroundService.pauseLock.wait();
                    } catch (InterruptedException ignored) {}
                }
            }

            socketOs.write(buffer, 0, readBytes);
            totalBytesSent += readBytes;
            speedPeriodBytesSent += readBytes;
            FileTransferForegroundService.lastPosition = totalBytesSent;

            long currentTime = System.currentTimeMillis();
            long timeDiff = currentTime - speedPeriodStartTime;
            if (timeDiff >= 1000) {
                currentSpeed = ((double) speedPeriodBytesSent / (1024.0 * 1024.0)) / ((double) timeDiff / 1000.0);
                speedPeriodBytesSent = 0;
                speedPeriodStartTime = currentTime;
            }

            if (currentTime - lastNotificationUpdateTime >= 1000) {
                int percent = (int) ((totalBytesSent * 100) / totalSize);
                broadcastProgress(fileName, totalBytesSent, totalSize, percent, fileIndex, currentSpeed);
                lastNotificationUpdateTime = currentTime;
            }
        }
        socketOs.flush();
    }

    /**
     * Backward-compatible standard sequential LZ4 file transmitter for files under 100MB.
     * Delegates natively to opening a socket and streaming as an isolated single file task.
     */
    public void sendSequentialFile(Uri fileUri, String fileName, long fileSize, String peerIp, int fileIndex) throws Exception {
        try (SocketChannel socketChannel = SocketChannel.open()) {
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setSendBufferSize(1024 * 1024);
            socketChannel.connect(new InetSocketAddress(peerIp, Constants.OFF_PORT_FILE));
            socketChannel.configureBlocking(true);
            sendSequentialFilePersistent(fileUri, fileName, fileSize, socketChannel, fileIndex);
        }
    }

    private File getBackupDirectory() {
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

    private ByteBuffer metadataBuffer(byte[] metaBytes) {
        return ByteBuffer.wrap(metaBytes);
    }

    private void broadcastProgress(String fileName, long transferred, long total, int percent, int fileIndex, double speed) {
        Intent intent = new Intent(Constants.ACTION_TRANSFER_PROGRESS);
        intent.putExtra(Constants.EXTRA_FILE_NAME, fileName);
        intent.putExtra(Constants.EXTRA_BYTES_TRANSFERRED, transferred);
        intent.putExtra(Constants.EXTRA_TOTAL_BYTES, total);
        intent.putExtra(Constants.EXTRA_FILE_INDEX, fileIndex);
        intent.putExtra(Constants.EXTRA_TRANSFER_SPEED, speed);
        intent.putExtra("IS_CHUNK", false);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);

        String speedText = String.format(Locale.US, "%.1f MB/s", speed);
        Intent serviceIntent = new Intent(context, FileTransferForegroundService.class);
        serviceIntent.setAction("UPDATE_NOTIF");
        serviceIntent.putExtra("NOTIF_TEXT", "Sending " + fileName + " (" + percent + "%) - " + speedText);
        serviceIntent.putExtra("PROGRESS", percent);
        context.startService(serviceIntent);
    }

    private void broadcastChunkProgress(String fileName, long transferred, long total, int percent, int fileIndex, double speed, int chunkIndex, long chunkTransferred, long chunkTotal) {
        Intent intent = new Intent(Constants.ACTION_TRANSFER_PROGRESS);
        intent.putExtra(Constants.EXTRA_FILE_NAME, fileName);
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
        serviceIntent.putExtra("NOTIF_TEXT", "Sending Chunk #" + (chunkIndex + 1) + " of " + fileName + " (" + percent + "%) - " + speedText);
        serviceIntent.putExtra("PROGRESS", percent);
        context.startService(serviceIntent);
    }

    private void broadcastCompletion() {
        Intent intent = new Intent(Constants.ACTION_TRANSFER_COMPLETE);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);
    }

    private void broadcastError(String message) {
        Intent intent = new Intent(Constants.ACTION_TRANSFER_ERROR);
        intent.putExtra(Constants.EXTRA_ERROR_MESSAGE, message);
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent);
    }

    private static class NonClosingOutputStream extends OutputStream {
        private final OutputStream delegate;

        public NonClosingOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            Log.d(TAG, "Intercepted close() request. Stream remains open.");
        }
    }
}