package com.wife.app;

import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.util.Log;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CallSignalingManager {
    private static final String TAG = "CallSignalingManager";
    private static volatile CallSignalingManager instance;

    private final Context context;
    private final ExecutorService executorService;
    private final List<SignalingEventListener> listeners = new ArrayList<>();

    public interface SignalingEventListener {
        void onSignalReceived(String action, String peerIp, JsonObject payload);
    }

    public static CallSignalingManager getInstance(Context context) {
        if (instance == null) {
            synchronized (CallSignalingManager.class) {
                if (instance == null) {
                    instance = new CallSignalingManager(context.getApplicationContext());
                }
            }
        }
        return instance;
    }

    private CallSignalingManager(Context context) {
        this.context = context;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public synchronized void registerListener(SignalingEventListener listener) {
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    public synchronized void unregisterListener(SignalingEventListener listener) {
        listeners.remove(listener);
    }

    public void sendSignal(final String peerIp, final String type) {
        WifeLogger.log(TAG, "sendSignal() invoked. Queuing outbound control signal: " + type + " | Target IP: " + peerIp);
        executorService.execute(() -> {
            WifeLogger.log(TAG, "Executing outbound signaling socket task on background worker thread.");
            try (Socket socket = new Socket(peerIp, Constants.OFF_PORT_CONTROL);
                 OutputStream os = socket.getOutputStream();
                 PrintWriter pw = new PrintWriter(os, true)) {

                WifeLogger.log(TAG, "Signaling socket connected successfully with " + peerIp + " on control Port: " + Constants.OFF_PORT_CONTROL);

                SharedPreferences prefs = context.getSharedPreferences("WifeSettings", Context.MODE_PRIVATE);
                String customName = prefs.getString("custom_alias", Utils.getDeviceModel());
                boolean isPublic = prefs.getBoolean("profile_privacy_public", true);

                JsonObject json = new JsonObject();
                json.addProperty("type", type);
                json.addProperty("sender", Utils.getDeviceId(context));
                json.addProperty("senderName", customName);

                // Broadcast local profile photo bytes only if privacy settings allow
                if (isPublic) {
                    String base64Photo = ProfileImageManager.getLocalProfileImageBase64(context);
                    if (base64Photo != null && !base64Photo.isEmpty()) {
                        WifeLogger.log(TAG, "Appending compressed Base64 profile photo bytes to signaling JSON payload.");
                        json.addProperty("profile_photo", base64Photo);
                    } else {
                        WifeLogger.log(TAG, "Public profile broadcasting is enabled, but no local profile image was discovered.");
                    }
                } else {
                    WifeLogger.log(TAG, "Broadcasting profile photo bypassed: User settings configured as PRIVATE.");
                }

                String payload = json.toString();
                WifeLogger.log(TAG, "Transmitting finalized signaling payload block. Action: " + type);

                pw.println(payload);
                pw.flush();
                Log.d(TAG, "Sent signal: " + type + " to IP: " + peerIp);
                WifeLogger.log(TAG, "Outbound signaling packet successfully written and flushed.");

            } catch (Exception e) {
                Log.e(TAG, "Failed sending signaling packet: " + e.getMessage());
                WifeLogger.log(TAG, "Failed transmitting outbound control signal " + type + " to IP " + peerIp + " | Exception: " + e.getMessage(), e);
            }
        });
    }

    // Overloaded method to support sending target timestamps for unsend signals
    public void sendSignal(final String peerIp, final String type, final long timestamp) {
        WifeLogger.log(TAG, "sendSignal() with timestamp invoked. Queuing outbound control signal: " + type + " | Target IP: " + peerIp + " | Timestamp: " + timestamp);
        executorService.execute(() -> {
            WifeLogger.log(TAG, "Executing outbound signaling socket task on background worker thread.");
            try (Socket socket = new Socket(peerIp, Constants.OFF_PORT_CONTROL);
                 OutputStream os = socket.getOutputStream();
                 PrintWriter pw = new PrintWriter(os, true)) {

                WifeLogger.log(TAG, "Signaling socket connected successfully with " + peerIp + " on control Port: " + Constants.OFF_PORT_CONTROL);

                SharedPreferences prefs = context.getSharedPreferences("WifeSettings", Context.MODE_PRIVATE);
                String customName = prefs.getString("custom_alias", Utils.getDeviceModel());

                JsonObject json = new JsonObject();
                json.addProperty("type", type);
                json.addProperty("sender", Utils.getDeviceId(context));
                json.addProperty("senderName", customName);
                json.addProperty("timestamp", timestamp); // Attach the targeted message timestamp

                String payload = json.toString();
                WifeLogger.log(TAG, "Transmitting finalized signaling payload block. Action: " + type);

                pw.println(payload);
                pw.flush();
                Log.d(TAG, "Sent signal: " + type + " with timestamp to IP: " + peerIp);
                WifeLogger.log(TAG, "Outbound signaling packet with timestamp successfully written and flushed.");

            } catch (Exception e) {
                Log.e(TAG, "Failed sending signaling packet with timestamp: " + e.getMessage());
                WifeLogger.log(TAG, "Failed transmitting outbound control signal with timestamp " + type + " to IP " + peerIp + " | Exception: " + e.getMessage(), e);
            }
        });
    }

    // Parallel decoupled 5-way Group Call Signaling Broadcaster helper
    public void broadcastGroupSignal(final List<String> peerIps, final String type) {
        if (peerIps == null || peerIps.isEmpty()) return;
        WifeLogger.log(TAG, "broadcastGroupSignal() invoked. Action: " + type + " | Target IP count: " + peerIps.size());
        for (String ip : peerIps) {
            sendSignal(ip, type);
        }
    }

    // Parallel mesh roster synchronization broadcaster helper for targeted transfers
    public void broadcastRosterSync(final List<String> peerIps, final java.util.Map<String, String> syncMap) {
        if (peerIps == null || peerIps.isEmpty() || syncMap == null) return;
        WifeLogger.log(TAG, "broadcastRosterSync() invoked. Target IP count: " + peerIps.size());
        executorService.execute(() -> {
            try {
                Gson gson = new Gson();
                String rosterJson = gson.toJson(syncMap);
                for (String ip : peerIps) {
                    try (Socket socket = new Socket(ip, Constants.OFF_PORT_CONTROL);
                         OutputStream os = socket.getOutputStream();
                         PrintWriter pw = new PrintWriter(os, true)) {

                        JsonObject json = new JsonObject();
                        json.addProperty("type", Constants.SIGNAL_PEER_ROSTER_SYNC);
                        json.addProperty("sender", Utils.getDeviceId(context));
                        json.addProperty("roster", rosterJson);

                        String payload = json.toString();
                        pw.println(payload);
                        pw.flush();
                        Log.d(TAG, "Sent roster sync packet to: " + ip);
                    } catch (Exception e) {
                        Log.e(TAG, "Failed sending roster sync to " + ip + ": " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                WifeLogger.log(TAG, "Error executing roster sync broadcast task: " + e.getMessage(), e);
            }
        });
    }

    public void handleReceivedSignal(String action, JsonObject payload, String peerIp) {
        Log.d(TAG, "Handling received signal: " + action + " from peer " + peerIp);
        WifeLogger.log(TAG, "handleReceivedSignal() invoked. Action: " + action + " | Peer IP: " + peerIp);
        
        // Handle roster sync signal in background before routing to active UI call screens
        if (Constants.SIGNAL_PEER_ROSTER_SYNC.equals(action)) {
            WifeLogger.log(TAG, "Signal matched: PEER_ROSTER_SYNC. Updating ConnectionManager.");
            try {
                if (payload != null && payload.has("roster")) {
                    String rosterJson = payload.get("roster").getAsString();
                    java.lang.reflect.Type mapType = new TypeToken<java.util.HashMap<String, String>>() {}.getType();
                    java.util.Map<String, String> receivedMap = new Gson().fromJson(rosterJson, mapType);

                    // Filter out local device to prevent looped messaging back to ourselves
                    String selfId = Utils.getDeviceId(context);
                    receivedMap.remove(selfId);

                    ConnectionManager.getInstance(context).syncGroupPeers(receivedMap);
                }
            } catch (Exception e) {
                WifeLogger.log(TAG, "Failed parsing synchronized mesh roster: " + e.getMessage(), e);
            }
            return; // Exit signaling flow for group setup sync packets
        }

        // Notify any active call screen UI
        synchronized (this) {
            if (!listeners.isEmpty()) {
                WifeLogger.log(TAG, "Active Call Screen listener discovered. Relaying control event directly.");
                for (SignalingEventListener listener : listeners) {
                    listener.onSignalReceived(action, peerIp, payload);
                }
                return;
            }
        }

        // Standard actions if no listener is currently registered (e.g., launching call UI when background/idle)
        WifeLogger.log(TAG, "No active foreground call listeners. Assessing background calling intents...");
        
        String peerName = payload.has("senderName") ? payload.get("senderName").getAsString() : "Video Peer";
        String base64Photo = payload.has("profile_photo") ? payload.get("profile_photo").getAsString() : "";

        if (Constants.SIGNAL_CALL_REQUEST.equals(action)) {
            WifeLogger.log(TAG, "Background event matched: 'SIGNAL_CALL_REQUEST'. Creating VoiceCallActivity calling intent.");
            Intent callIntent = new Intent(context, VoiceCallActivity.class);
            callIntent.putExtra(Constants.EXTRA_PEER_IP, peerIp);
            callIntent.putExtra(Constants.EXTRA_PEER_NAME, peerName);
            callIntent.putExtra("PEER_PROFILE_PHOTO", base64Photo);
            callIntent.putExtra("IS_INBOUND", true);
            callIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            context.startActivity(callIntent);
            WifeLogger.log(TAG, "VoiceCallActivity incoming call background intent dispatched successfully.");
            
        } else if (Constants.SIGNAL_VIDEO_REQUEST.equals(action)) {
            WifeLogger.log(TAG, "Background event matched: 'SIGNAL_VIDEO_REQUEST'. Creating VideoCallActivity calling intent.");
            Intent videoIntent = new Intent(context, VideoCallActivity.class);
            videoIntent.putExtra(Constants.EXTRA_PEER_IP, peerIp);
            videoIntent.putExtra(Constants.EXTRA_PEER_NAME, peerName);
            videoIntent.putExtra("PEER_PROFILE_PHOTO", base64Photo);
            videoIntent.putExtra("IS_INBOUND", true);
            videoIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            context.startActivity(videoIntent);
            WifeLogger.log(TAG, "VideoCallActivity incoming call background intent dispatched successfully.");
        } else if (Constants.SIGNAL_GROUP_CALL_REQUEST.equals(action)) {
            WifeLogger.log(TAG, "Background event matched: 'SIGNAL_GROUP_CALL_REQUEST'. Creating GroupVideoCallActivity calling intent.");
            Intent groupIntent = new Intent(context, GroupVideoCallActivity.class);
            groupIntent.putExtra(Constants.EXTRA_PEER_IP, peerIp);
            groupIntent.putExtra(Constants.EXTRA_PEER_NAME, peerName);
            groupIntent.putExtra("PEER_PROFILE_PHOTO", base64Photo);
            groupIntent.putExtra("IS_INBOUND", true);
            groupIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            context.startActivity(groupIntent);
            WifeLogger.log(TAG, "GroupVideoCallActivity incoming call background intent dispatched successfully.");
        } else {
            WifeLogger.log(TAG, "Unprocessed background signaling control code ignored: " + action);
        }
    }
}