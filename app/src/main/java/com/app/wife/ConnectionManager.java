package com.wife.app;

import android.content.Context;
import android.net.wifi.p2p.WifiP2pInfo;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class ConnectionManager implements WiFiDirectManager.ConnectionChangeListener {
    private static final String TAG = "ConnectionManager";
    private static volatile ConnectionManager instance;

    private final Context context;
    private final List<ConnectionStatusListener> statusListeners = new ArrayList<>();

    private SocketServer socketServer;
    private SocketClient socketClient;

    private String peerIpAddress = "";
    private boolean isHost = false;
    private boolean isConnected = false;

    // Symmetrical cache variable to hold the active peer's unique hardware ID
    private String peerDeviceId = "";

    // Parallel decoupled multi-peer directory for 5-way group calling
    private final java.util.Map<String, String> groupPeers = new java.util.concurrent.ConcurrentHashMap<>();

    // Debounce variables to guard against transient disconnection states during multi-device handshakes
    private final Handler mainHandler = new Handler(Looper.getMainLooper());
    private Runnable pendingTeardownRunnable;
    private static final long TEARDOWN_DEBOUNCE_DELAY_MS = 2000;

    public interface ConnectionStatusListener {
        void onConnectionStateChanged(boolean connected, String peerIp, boolean isHost);
    }

    public static ConnectionManager getInstance(Context context) {
        if (instance == null) {
            synchronized (ConnectionManager.class) {
                if (instance == null) {
                    instance = new ConnectionManager(context.getApplicationContext());
                }
            }
        }
        return instance;
    }

    private ConnectionManager(Context context) {
        this.context = context;
        WiFiDirectManager.getInstance(context).registerConnectionChangeListener(this);
    }

    public synchronized void registerStatusListener(ConnectionStatusListener listener) {
        if (!statusListeners.contains(listener)) {
            statusListeners.add(listener);
        }
        listener.onConnectionStateChanged(isConnected, peerIpAddress, isHost);
    }

    public synchronized void unregisterStatusListener(ConnectionStatusListener listener) {
        statusListeners.remove(listener);
    }

    public String getPeerIpAddress() {
        return peerIpAddress;
    }

    public boolean isHost() {
        return isHost;
    }

    public boolean isConnected() {
        return isConnected;
    }

    // Thread-safe getter to expose the connected peer's unique hardware ID
    public synchronized String getPeerDeviceId() {
        return peerDeviceId;
    }

    // Thread-safe setter to update the active peer's unique hardware ID on handshake/messages
    public synchronized void setPeerDeviceId(String peerDeviceId) {
        WifeLogger.log(TAG, "setPeerDeviceId called. Tracking peer ID: " + peerDeviceId);
        this.peerDeviceId = peerDeviceId;
        
        // For a multi-client group calling host, register this mapped identity to our group peers directory
        if (isHost && peerIpAddress != null && !peerIpAddress.isEmpty()) {
            addGroupPeer(peerDeviceId, peerIpAddress);
        }
    }

    // Parallel multi-peer registration mapping for 5-way group calls
    public synchronized void addGroupPeer(String deviceId, String ipAddress) {
        if (deviceId != null && !deviceId.isEmpty() && ipAddress != null && !ipAddress.isEmpty()) {
            // Symmetrical Deduplication: Remove any previous temporary keys pointing to the same IP value
            List<String> keysToRemove = new ArrayList<>();
            for (java.util.Map.Entry<String, String> entry : groupPeers.entrySet()) {
                if (ipAddress.equals(entry.getValue())) {
                    keysToRemove.add(entry.getKey());
                }
            }
            for (String key : keysToRemove) {
                groupPeers.remove(key);
            }
            groupPeers.put(deviceId, ipAddress);
            WifeLogger.log(TAG, "addGroupPeer: Tracked " + deviceId + " at IP: " + ipAddress + " | Group Size: " + groupPeers.size());
            
            // Sync current active mesh directory with all connected nodes
            triggerRosterSyncBroadcast();
        }
    }

    // Parallel multi-peer removal mapping for 5-way group calls
    public synchronized void removeGroupPeer(String deviceId) {
        if (deviceId != null) {
            groupPeers.remove(deviceId);
            WifeLogger.log(TAG, "removeGroupPeer: Untracked " + deviceId + " | Group Size: " + groupPeers.size());
            
            // Sync current active mesh directory with all connected nodes
            triggerRosterSyncBroadcast();
        }
    }

    // Exposes a thread-safe copy of currently active group peers
    public synchronized java.util.Map<String, String> getGroupPeers() {
        return new java.util.HashMap<>(groupPeers);
    }

    // Thread-safe roster sync receiver for client nodes
    public synchronized void syncGroupPeers(java.util.Map<String, String> newPeers) {
        groupPeers.clear();
        if (newPeers != null) {
            groupPeers.putAll(newPeers);
        }
        WifeLogger.log(TAG, "syncGroupPeers: Local roster updated. Active Count: " + groupPeers.size());
    }

    // Helper to package and broadcast Host directory changes to all Client nodes over Port 8888
    private void triggerRosterSyncBroadcast() {
        if (isHost) {
            WifeLogger.log(TAG, "Triggering mesh roster synchronization broadcast.");
            java.util.Map<String, String> syncMap = new java.util.HashMap<>(groupPeers);
            syncMap.put(Utils.getDeviceId(context), "192.168.49.1"); // Inject Host device configuration into list
            
            CallSignalingManager.getInstance(context).broadcastRosterSync(
                new ArrayList<>(groupPeers.values()),
                syncMap
            );
        }
    }

    @Override
    public void onConnectionChanged(WifiP2pInfo info) {
        if (info != null && info.groupFormed) {
            // Cancel any scheduled connection teardowns since the link is stable and formed
            cancelPendingTeardown();

            // Symmetrical State Guard: Only initiate servers and sockets on a fresh link connection
            if (!isConnected) {
                isConnected = true;
                isHost = info.isGroupOwner;
                
                // Critical Fix: BOTH Host and Client must start background socket servers
                // to listen for incoming bidirectional message and calling requests.
                startServers();
                
                if (isHost) {
                    Log.d(TAG, "Device is Group Owner. Starting SocketServers...");
                    WifeLogger.log(TAG, "P2P connection established. This device is the Group Owner (Host). Server sockets started.");
                    peerIpAddress = ""; // Will be updated when Client connects to Control Server
                } else {
                    Log.d(TAG, "Device is Client. Connecting to Host: " + info.groupOwnerAddress.getHostAddress());
                    WifeLogger.log(TAG, "P2P connection established. This device is the Client. Server sockets started. Connecting to Host: " + info.groupOwnerAddress.getHostAddress());
                    peerIpAddress = info.groupOwnerAddress.getHostAddress();
                    startClient(info.groupOwnerAddress);
                }
            } else {
                WifeLogger.log(TAG, "onConnectionChanged: Link already active. Ignoring server recreation to preserve client sockets.");
            }
        } else {
            // Delay teardown to prevent transient disconnect signals from dismantling active sockets
            scheduleTeardownWithDelay();
        }
        notifyStateChanged();
    }

    private synchronized void startServers() {
        WifeLogger.log(TAG, "startServers() invoked. Initializing SocketServer threads...");
        if (socketServer != null) {
            socketServer.stop();
        }
        socketServer = new SocketServer(context, this);
        socketServer.start();

        // Start the persistent NIO file receiver on port 8900
        WifeLogger.log(TAG, "startServers() invoking persistent high-speed FileReceiver.startServer.");
        FileReceiver.startServer(context);
    }

    private synchronized void startClient(InetAddress hostAddress) {
        WifeLogger.log(TAG, "startClient() invoked. Initializing SocketClient targeting Host: " + hostAddress.getHostAddress());
        if (socketClient != null) {
            socketClient.close();
        }
        socketClient = new SocketClient(context, hostAddress, this);
        socketClient.start();
    }

    public synchronized void updatePeerIpFromAccept(String acceptedIp) {
        WifeLogger.log(TAG, "updatePeerIpFromAccept called with IP: " + acceptedIp + ". Current cached Peer IP: " + peerIpAddress);
        if (peerIpAddress == null || peerIpAddress.isEmpty() || !peerIpAddress.equals(acceptedIp)) {
            Log.d(TAG, "Host recorded Client IP: " + acceptedIp);
            WifeLogger.log(TAG, "Updating Peer IP Address reference to accepted client socket IP: " + acceptedIp);
            this.peerIpAddress = acceptedIp;
            notifyStateChanged();
        }
        
        // Symmetrical mapping: In a multi-client AGO group, we dynamically register 
        // every accepted Client IP into our Group Peers map so GroupCallManager can target them
        if (isHost && acceptedIp != null && !acceptedIp.isEmpty()) {
            addGroupPeer("peer_" + acceptedIp.replace(".", "_"), acceptedIp);
        }
    }

    private synchronized void scheduleTeardownWithDelay() {
        if (pendingTeardownRunnable != null) {
            return;
        }
        WifeLogger.log(TAG, "Disconnect event reported. Scheduling socket teardown with " + TEARDOWN_DEBOUNCE_DELAY_MS + "ms delay to guard against transient signals.");
        pendingTeardownRunnable = () -> {
            synchronized (ConnectionManager.this) {
                WifeLogger.log(TAG, "Debounce timer elapsed. Executing final socket teardown.");
                teardown();
                pendingTeardownRunnable = null;
            }
        };
        mainHandler.postDelayed(pendingTeardownRunnable, TEARDOWN_DEBOUNCE_DELAY_MS);
    }

    private synchronized void cancelPendingTeardown() {
        if (pendingTeardownRunnable != null) {
            WifeLogger.log(TAG, "P2P connection stabilized. Cancelling pending socket teardown task.");
            mainHandler.removeCallbacks(pendingTeardownRunnable);
            pendingTeardownRunnable = null;
        }
    }

    public synchronized void teardown() {
        WifeLogger.log(TAG, "teardown() invoked. Cleared connection state variables.");
        
        // Cancel any pending scheduled teardowns to avoid double execution
        if (pendingTeardownRunnable != null) {
            mainHandler.removeCallbacks(pendingTeardownRunnable);
            pendingTeardownRunnable = null;
        }

        isConnected = false;
        peerIpAddress = "";
        peerDeviceId = ""; // Clear active peer device ID on connection loss
        groupPeers.clear(); // Clean up parallel group call peer mapping directories
        isHost = false;

        // Stop any running file transfer activities/servers cleanly
        FileTransferForegroundService.isCancelled = true;
        synchronized (FileTransferForegroundService.pauseLock) {
            FileTransferForegroundService.pauseLock.notifyAll();
        }
        
        if (socketServer != null) {
            socketServer.stop();
            socketServer = null;
        }
        if (socketClient != null) {
            socketClient.close();
        }

        // Programmatically destroy the virtual P2P group at the OS level to prevent stale connected status freezing
        WiFiDirectManager.getInstance(context).disconnect(null);

        notifyStateChanged();
    }

    private void notifyStateChanged() {
        new Handler(Looper.getMainLooper()).post(() -> {
            List<ConnectionStatusListener> targets;
            synchronized (ConnectionManager.this) {
                targets = new ArrayList<>(statusListeners);
            }
            WifeLogger.log(TAG, "Dispatching connection state change. Connected: " + isConnected + ", Peer IP: " + peerIpAddress + ", Is Host: " + isHost);
            for (ConnectionStatusListener listener : targets) {
                listener.onConnectionStateChanged(isConnected, peerIpAddress, isHost);
            }
        });
    }
}