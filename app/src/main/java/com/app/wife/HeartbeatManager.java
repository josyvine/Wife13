package com.wife.app;

import android.content.Context;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HeartbeatManager {
    private static final String TAG = "HeartbeatManager";
    private static volatile HeartbeatManager instance;

    private final Context context;
    private ScheduledExecutorService scheduler;
    private final Handler mainHandler;

    private ScheduledFuture<?> sendTask;
    private ScheduledFuture<?> checkTask;

    private long lastHeartbeatReceived = 0;
    private boolean isMonitoring = false;
    private volatile boolean initialGracePeriodActive = true;

    public static HeartbeatManager getInstance(Context context) {
        if (instance == null) {
            synchronized (HeartbeatManager.class) {
                if (instance == null) {
                    instance = new HeartbeatManager(context.getApplicationContext());
                }
            }
        }
        return instance;
    }

    private HeartbeatManager(Context context) {
        this.context = context;
        this.scheduler = new ScheduledThreadPoolExecutor(1);
        this.mainHandler = new Handler(Looper.getMainLooper());
    }

    public synchronized void startMonitoring() {
        if (isMonitoring) return;
        isMonitoring = true;
        lastHeartbeatReceived = System.currentTimeMillis();
        initialGracePeriodActive = true;

        // Dynamically re-create the scheduler if it was previously shut down, closed, or terminated
        if (scheduler == null || scheduler.isShutdown() || scheduler.isTerminated()) {
            scheduler = new ScheduledThreadPoolExecutor(1);
        }

        // Send heartbeat packet every 5 seconds
        sendTask = scheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, 5, TimeUnit.SECONDS);

        // Check for heartbeat failures every 5 seconds
        checkTask = scheduler.scheduleAtFixedRate(this::checkHeartbeatStatus, 5, 5, TimeUnit.SECONDS);
        Log.d(TAG, "Heartbeat monitor launched.");
    }

    public synchronized void stopMonitoring() {
        isMonitoring = false;
        initialGracePeriodActive = true;

        // Cancel the individual running tasks
        if (sendTask != null) {
            sendTask.cancel(true);
            sendTask = null;
        }
        if (checkTask != null) {
            checkTask.cancel(true);
            checkTask = null;
        }

        // Cleanly shut down the scheduler to release system resources and prevent task leaks
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }

        Log.d(TAG, "Heartbeat monitoring stopped.");
    }

    private void sendHeartbeat() {
        ConnectionManager conn = ConnectionManager.getInstance(context);
        if (conn.isHost()) {
            // Host pings all connected clients to keep their connections alive
            for (String ip : conn.getGroupPeers().values()) {
                if (ip != null && !ip.isEmpty()) {
                    CallSignalingManager.getInstance(context).sendSignal(ip, "heartbeat");
                }
            }
        } else {
            // Client pings the Group Owner (Host)
            String peerIp = conn.getPeerIpAddress();
            if (peerIp != null && !peerIp.isEmpty()) {
                CallSignalingManager.getInstance(context).sendSignal(peerIp, "heartbeat");
            }
        }
    }

    private void checkHeartbeatStatus() {
        if (!isMonitoring) return;
        
        long diff = System.currentTimeMillis() - lastHeartbeatReceived;
        long timeoutLimit = initialGracePeriodActive ? 40000L : 15000L;

        if (diff > timeoutLimit) { // Timeout evaluated dynamically based on connection state
            Log.e(TAG, "Heartbeat timeout! Peer disconnected. Elapsed: " + diff + "ms (Limit: " + timeoutLimit + "ms)");
            mainHandler.post(() -> {
                // Terminate connections and trigger auto-reconnection
                ConnectionManager.getInstance(context).teardown();
                ReconnectManager.getInstance(context).triggerReconnect();
            });
        }
    }

    public synchronized void onHeartbeatReceived(String peerIp) {
        lastHeartbeatReceived = System.currentTimeMillis();
        initialGracePeriodActive = false; // Initial proof of life obtained, strict 15-second watchdog is now active
        Log.d(TAG, "Heartbeat received from " + peerIp);
    }
}