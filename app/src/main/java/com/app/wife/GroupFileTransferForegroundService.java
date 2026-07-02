package com.wife.app;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.Service;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.pm.ServiceInfo;
import android.net.Uri;
import android.os.Build;
import android.os.Environment;
import android.os.IBinder;
import android.util.Log;

import androidx.annotation.Nullable;
import androidx.core.app.NotificationCompat;
import androidx.localbroadcastmanager.content.LocalBroadcastManager;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupFileTransferForegroundService extends Service {
    private static final String TAG = "GroupFileTransferService";
    private static final String CHANNEL_ID = "WifeGroupFileTransferChannel";
    private static final int NOTIF_ID = 1006;

    // --- Symmetrical Monitor Locks & Shared Volatile State for Group Transfers ---
    public static final Object pauseLock = new Object();
    public static volatile boolean isPaused = false;
    public static volatile boolean isCancelled = false;
    public static volatile boolean isUserCancelled = false; // Tracks explicit user cancellation
    public static volatile long lastPosition = 0;

    // Global transaction reference counter to track parallel group sending/receiving sessions
    public static final AtomicInteger activeTransfersCount = new AtomicInteger(0);

    // System IPC Throttling variables
    private static final long NOTIFICATION_THROTTLE_MS = 2000;
    private static volatile long lastNotificationTime = 0;

    // Broadcast receiver to pause transfers during active voice/video calls to prioritize bandwidth
    private final BroadcastReceiver callStateReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            String action = intent.getAction();
            if (action == null) return;

            if ("com.wife.app.ACTION_CALL_ACTIVE".equals(action)) {
                WifeLogger.log(TAG, "Call active broadcast received. Suspending group background transfer streams.");
                isPaused = true;
                updateNotification("Group transfer suspended due to active call", 0, true);
            } else if ("com.wife.app.ACTION_CALL_INACTIVE".equals(action)) {
                WifeLogger.log(TAG, "Call ended broadcast received. Resuming suspended group transfer streams.");
                isPaused = false;
                synchronized (pauseLock) {
                    pauseLock.notifyAll();
                }
                updateNotification("Resuming group transfer streams...", 0, true);
            }
        }
    };

    /**
     * Decentralized termination controller. Decrements the active transaction count
     * and stops the service only when all parallel group connections have cleanly ended.
     */
    public static void decrementAndCheckStop(Context context) {
        int active = activeTransfersCount.decrementAndGet();
        if (active < 0) {
            activeTransfersCount.set(0);
            active = 0;
        }
        WifeLogger.log(TAG, "decrementAndCheckStop() executed. Remaining active group transfer sessions: " + active);
        if (active == 0) {
            WifeLogger.log(TAG, "No active group transactions remaining. Halting GroupFileTransferForegroundService.");
            Intent stopIntent = new Intent(context, GroupFileTransferForegroundService.class);
            context.stopService(stopIntent);
        }
    }

    @Override
    public void onCreate() {
        super.onCreate();
        createNotificationChannel();
        WifeLogger.log(TAG, "onCreate() invoked. GroupFileTransferForegroundService initialized.");

        // Register the call-state mutual exclusion receiver
        IntentFilter filter = new IntentFilter();
        filter.addAction("com.wife.app.ACTION_CALL_ACTIVE");
        filter.addAction("com.wife.app.ACTION_CALL_INACTIVE");
        LocalBroadcastManager.getInstance(this).registerReceiver(callStateReceiver, filter);
        WifeLogger.log(TAG, "Group call-state mutual exclusion receiver registered successfully.");
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent == null) {
            WifeLogger.log(TAG, "onStartCommand() received null Intent. Stopping service.");
            stopSelf();
            return START_NOT_STICKY;
        }

        String action = intent.getAction();
        WifeLogger.log(TAG, "onStartCommand() triggered with Action: " + (action == null ? "None" : action));

        if (action != null) {
            switch (action) {
                case Constants.ACTION_GROUP_START_TRANSFER:
                    isCancelled = false;
                    isPaused = false;
                    isUserCancelled = false; // Reset manual cancel flag
                    lastPosition = 0;
                    lastNotificationTime = 0; // Reset throttling mark

                    boolean isSender = intent.getBooleanExtra("IS_SENDER", false);
                    WifeLogger.log(TAG, "ACTION_GROUP_START_TRANSFER initiated. Transfer Role: " + (isSender ? "Sender" : "Receiver"));

                    Notification notification = buildProgressNotification("Initializing group transfer stream...", 0, true);
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                        startForeground(NOTIF_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC);
                    } else {
                        startForeground(NOTIF_ID, notification);
                    }

                    if (isSender) {
                        ArrayList<String> uriStrings = intent.getStringArrayListExtra("URI_LIST");
                        ArrayList<String> fileNames = intent.getStringArrayListExtra("FILE_NAMES");
                        long[] fileSizes = intent.getLongArrayExtra("FILE_SIZES");
                        ArrayList<String> targetIps = intent.getStringArrayListExtra("TARGET_IPS");

                        if (uriStrings != null && !uriStrings.isEmpty() && targetIps != null && !targetIps.isEmpty()) {
                            ArrayList<Uri> uris = new ArrayList<>();
                            for (String uriStr : uriStrings) {
                                uris.add(Uri.parse(uriStr));
                            }
                            WifeLogger.log(TAG, "Spawning high-speed GroupFileSender thread pool. Queue size: " + uris.size());
                            GroupFileSender.getInstance(this).sendGroupQueue(uris, fileNames, fileSizes, targetIps);
                        } else {
                            WifeLogger.log(TAG, "Aborted group sender initialization: Empty file queue or missing target IPs.");
                            stopSelf();
                        }
                    } else {
                        WifeLogger.log(TAG, "Spawning Group ServerSocketChannel persistent receiver thread.");
                        GroupFileReceiver.startServer(this);
                    }
                    break;

                case "UPDATE_NOTIF":
                    if (isCancelled) {
                        WifeLogger.log(TAG, "UPDATE_NOTIF ignored: Group transfer session is inactive. Stopping service.");
                        stopSelf();
                        break;
                    }

                    String notifText = intent.getStringExtra("NOTIF_TEXT");
                    int progressValue = intent.getIntExtra("PROGRESS", 0);

                    Notification initNotif = buildProgressNotification(notifText, progressValue, false);
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                        startForeground(NOTIF_ID, initNotif, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC);
                    } else {
                        startForeground(NOTIF_ID, initNotif);
                    }

                    // Enforces a maximum rate of 1 IPC transaction every 2 seconds to prevent ANR freezes
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastNotificationTime >= NOTIFICATION_THROTTLE_MS) {
                        updateNotification(notifText, progressValue, false);
                        lastNotificationTime = currentTime;
                    }
                    break;

                case Constants.ACTION_GROUP_PAUSE_TRANSFER:
                    WifeLogger.log(TAG, "ACTION_GROUP_PAUSE_TRANSFER received. Suspending group file stream threads.");
                    isPaused = true;
                    updateNotification("Group Transfer Paused", 0, true);
                    break;

                case Constants.ACTION_GROUP_RESUME_TRANSFER:
                    WifeLogger.log(TAG, "ACTION_GROUP_RESUME_TRANSFER received. Notifying group waiting locks.");
                    isPaused = false;
                    synchronized (pauseLock) {
                        pauseLock.notifyAll();
                    }
                    updateNotification("Resuming group transfer stream...", 0, true);
                    break;

                case Constants.ACTION_GROUP_CANCEL_TRANSFER:
                    WifeLogger.log(TAG, "ACTION_GROUP_CANCEL_TRANSFER received. Purging group sockets and shutting down.");
                    isCancelled = true;
                    isPaused = false;
                    isUserCancelled = true; // Flag manual user abort
                    synchronized (pauseLock) {
                        pauseLock.notifyAll();
                    }

                    Intent cancelIntent = new Intent(Constants.ACTION_GROUP_TRANSFER_ERROR);
                    cancelIntent.putExtra(Constants.EXTRA_ERROR_MESSAGE, "Group transfer cancelled by user.");
                    LocalBroadcastManager.getInstance(this).sendBroadcast(cancelIntent);

                    stopForeground(true);
                    stopSelf();
                    break;

                default:
                    WifeLogger.log(TAG, "Unrecognized action passed to group service: " + action);
                    break;
            }
        }
        return START_NOT_STICKY;
    }

    public void updateNotification(String contentText, int progress, boolean indeterminate) {
        NotificationManager manager = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        if (manager != null && !isCancelled) {
            Notification notification = buildProgressNotification(contentText, progress, indeterminate);
            manager.notify(NOTIF_ID, notification);
        }
    }

    private Notification buildProgressNotification(String contentText, int progress, boolean indeterminate) {
        NotificationCompat.Builder builder = new NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle("Wife Group File Sharing")
                .setContentText(contentText)
                .setSmallIcon(android.R.drawable.stat_sys_download_done)
                .setOnlyAlertOnce(true)
                .setOngoing(true);

        if (indeterminate) {
            builder.setProgress(0, 0, true);
        } else {
            builder.setProgress(100, progress, false);
        }
        return builder.build();
    }

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(
                    CHANNEL_ID,
                    "Wife Group File Sharing Service",
                    NotificationManager.IMPORTANCE_DEFAULT
            );
            NotificationManager manager = getSystemService(NotificationManager.class);
            if (manager != null) {
                manager.createNotificationChannel(channel);
                WifeLogger.log(TAG, "Wife Group File Sharing Service Notification Channel created.");
            }
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

    @Override
    public void onDestroy() {
        WifeLogger.log(TAG, "onDestroy() invoked. Tearing down group file transfer service and cleaning resources.");

        stopForeground(true);

        NotificationManager manager = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        if (manager != null) {
            manager.cancel(NOTIF_ID);
        }

        isCancelled = true;
        isPaused = false;

        // Unregister the call-state mutual exclusion receiver safely
        try {
            LocalBroadcastManager.getInstance(this).unregisterReceiver(callStateReceiver);
            WifeLogger.log(TAG, "Group call-state receiver unregistered cleanly.");
        } catch (Exception e) {
            WifeLogger.log(TAG, "Error unregistering group call-state receiver: " + e.getMessage());
        }

        synchronized (pauseLock) {
            pauseLock.notifyAll();
        }

        // Force-purge temporary cache files from internal directory
        File cacheDir = getCacheDir();
        if (cacheDir != null && cacheDir.exists()) {
            File[] files = cacheDir.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.getName().startsWith("temp_group_")) {
                        boolean deleted = f.delete();
                        WifeLogger.log(TAG, "Purged internal group temp file: " + f.getName() + " | Status: " + deleted);
                    }
                }
            }
        }

        // Force-purge temporary LZ4 segments from external directory on manual user cancellation
        if (isUserCancelled) {
            File backupDir = getBackupDirectory();
            if (backupDir.exists()) {
                File[] files = backupDir.listFiles();
                if (files != null) {
                    for (File f : files) {
                        if (f.getName().startsWith("temp_send_group_") || f.getName().startsWith("temp_group_recv_")) {
                            boolean deleted = f.delete();
                            WifeLogger.log(TAG, "Purged external group segment cache file: " + f.getName() + " | Status: " + deleted);
                        }
                    }
                }
            }
        }

        isUserCancelled = false; // Reset manual cancellation flag
        super.onDestroy();
    }
}