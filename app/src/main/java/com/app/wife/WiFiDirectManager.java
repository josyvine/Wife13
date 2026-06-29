package com.wife.app;

import android.annotation.SuppressLint;
import android.content.Context;
import android.net.MacAddress;
import android.net.wifi.p2p.WifiP2pConfig;
import android.net.wifi.p2p.WifiP2pDevice;
import android.net.wifi.p2p.WifiP2pDeviceList;
import android.net.wifi.p2p.WifiP2pInfo;
import android.net.wifi.p2p.WifiP2pManager;
import android.os.Build;
import android.os.Looper;
import android.util.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class WiFiDirectManager {
    private static final String TAG = "WiFiDirectManager";
    private static volatile WiFiDirectManager instance;

    private final WifiP2pManager p2pManager;
    private final WifiP2pManager.Channel channel;
    private final List<WifiP2pDevice> peersList = new ArrayList<>();
    private WifiP2pInfo connectionInfo;

    public interface PeerChangeListener {
        void onPeersChanged(List<WifiP2pDevice> peers);
    }

    public interface ConnectionChangeListener {
        void onConnectionChanged(WifiP2pInfo info);
    }

    private final List<PeerChangeListener> peerListeners = new ArrayList<>();
    private final List<ConnectionChangeListener> connectionListeners = new ArrayList<>();

    public static WiFiDirectManager getInstance(Context context) {
        if (instance == null) {
            synchronized (WiFiDirectManager.class) {
                if (instance == null) {
                    instance = new WiFiDirectManager(context.getApplicationContext());
                }
            }
        }
        return instance;
    }

    private WiFiDirectManager(Context context) {
        p2pManager = (WifiP2pManager) context.getSystemService(Context.WIFI_P2P_SERVICE);
        channel = p2pManager.initialize(context, Looper.getMainLooper(), null);
    }

    public WifiP2pManager getP2pManager() {
        return p2pManager;
    }

    public WifiP2pManager.Channel getChannel() {
        return channel;
    }

    public List<WifiP2pDevice> getPeersList() {
        return peersList;
    }

    public WifiP2pInfo getConnectionInfo() {
        return connectionInfo;
    }

    public void registerPeerChangeListener(PeerChangeListener listener) {
        if (!peerListeners.contains(listener)) {
            peerListeners.add(listener);
        }
    }

    public void unregisterPeerChangeListener(PeerChangeListener listener) {
        peerListeners.remove(listener);
    }

    public void registerConnectionChangeListener(ConnectionChangeListener listener) {
        if (!connectionListeners.contains(listener)) {
            connectionListeners.add(listener);
        }
    }

    public void unregisterConnectionChangeListener(ConnectionChangeListener listener) {
        connectionListeners.remove(listener);
    }

    @SuppressLint("MissingPermission")
    public void discoverPeers() {
        if (p2pManager == null || channel == null) return;
        p2pManager.discoverPeers(channel, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                Log.d(TAG, "Peer discovery initiated successfully.");
            }

            @Override
            public void onFailure(int reason) {
                Log.e(TAG, "Peer discovery failed. Reason: " + reason);
            }
        });
    }

    @SuppressLint("MissingPermission")
    public void stopPeerDiscovery() {
        stopPeerDiscovery(null);
    }

    @SuppressLint("MissingPermission")
    public void stopPeerDiscovery(final WifiP2pManager.ActionListener listener) {
        if (p2pManager == null || channel == null) return;
        p2pManager.stopPeerDiscovery(channel, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                Log.d(TAG, "Peer discovery stopped successfully.");
                if (listener != null) {
                    listener.onSuccess();
                }
            }

            @Override
            public void onFailure(int reason) {
                Log.e(TAG, "Failed to stop peer discovery. Reason: " + reason);
                if (listener != null) {
                    listener.onFailure(reason);
                }
            }
        });
    }

    @SuppressLint("MissingPermission")
    public void connect(final WifiP2pDevice device, final WifiP2pManager.ActionListener listener) {
        if (p2pManager == null || channel == null || device == null) return;

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            try {
                MacAddress mac = MacAddress.fromString(device.deviceAddress);
                WifiP2pConfig config = new WifiP2pConfig.Builder()
                        .setDeviceAddress(mac)
                        .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_5GHZ)
                        .build();
                WifeLogger.log(TAG, "Forcing connection profile to prefer 5 GHz band for device: " + device.deviceName);
                p2pManager.connect(channel, config, listener);
            } catch (Exception e) {
                WifeLogger.log(TAG, "MacAddress builder parsing failed. Reverting to legacy connect profile. Error: " + e.getMessage());
                WifiP2pConfig config = new WifiP2pConfig();
                config.deviceAddress = device.deviceAddress;
                p2pManager.connect(channel, config, listener);
            }
        } else {
            WifiP2pConfig config = new WifiP2pConfig();
            config.deviceAddress = device.deviceAddress;
            p2pManager.connect(channel, config, listener);
        }
    }

    public void disconnect(final WifiP2pManager.ActionListener listener) {
        if (p2pManager == null || channel == null) return;
        p2pManager.removeGroup(channel, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                updateConnectionInfo(null);
                if (listener != null) {
                    listener.onSuccess();
                }
            }

            @Override
            public void onFailure(int reason) {
                if (listener != null) {
                    listener.onFailure(reason);
                }
            }
        });
    }

    public void createGroup(final WifiP2pManager.ActionListener listener) {
        if (p2pManager == null || channel == null) return;

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            WifiP2pConfig config = new WifiP2pConfig.Builder()
                    .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_5GHZ)
                    .build();
            WifeLogger.log(TAG, "Initiating 5 GHz band Autonomous P2P Group pre-creation.");
            p2pManager.createGroup(channel, config, listener);
        } else {
            p2pManager.createGroup(channel, listener);
        }
    }

    public void updatePeers(WifiP2pDeviceList deviceList) {
        peersList.clear();
        if (deviceList != null) {
            peersList.addAll(deviceList.getDeviceList());
        }
        for (PeerChangeListener listener : peerListeners) {
            listener.onPeersChanged(new ArrayList<>(peersList));
        }
    }

    public void updateConnectionInfo(WifiP2pInfo info) {
        this.connectionInfo = info;
        if (info == null || !info.groupFormed) {
            peersList.clear();
            for (PeerChangeListener listener : peerListeners) {
                listener.onPeersChanged(new ArrayList<>(peersList));
            }
        }
        for (ConnectionChangeListener listener : connectionListeners) {
            listener.onConnectionChanged(info);
        }
    }
}