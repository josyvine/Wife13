package com.wife.app;

import android.content.Intent;
import android.content.SharedPreferences;
import android.graphics.Bitmap;
import android.net.wifi.p2p.WifiP2pManager;
import android.os.Bundle;
import android.view.View;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;
import androidx.core.view.GravityCompat;

import com.wife.app.databinding.ActivityMainBinding;

public class MainActivity extends AppCompatActivity implements ConnectionManager.ConnectionStatusListener {

    private static final String TAG = "MainActivity";

    private ActivityMainBinding binding;
    private WiFiDirectManager wifiDirectManager;
    private ConnectionManager connectionManager;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = ActivityMainBinding.inflate(getLayoutInflater());
        setContentView(binding.getRoot());

        WifeLogger.log(TAG, "onCreate() invoked. Initializing MainActivity.");

        wifiDirectManager = WiFiDirectManager.getInstance(this);
        connectionManager = ConnectionManager.getInstance(this);

        setupMenuClickListeners();
        
        // Start foreground service to maintain socket control loop and global broadcast receiver
        WifeLogger.log(TAG, "Starting ConnectionForegroundService to manage global P2P events and sockets.");
        Intent serviceIntent = new Intent(this, ConnectionForegroundService.class);
        startService(serviceIntent);

        // Automatically restore all conversations asynchronously from public shared storage on startup (to survive clear data/reinstalls)
        new Thread(() -> {
            try {
                BackupManager.restoreAllChats(MainActivity.this);
            } catch (Exception e) {
                WifeLogger.log(TAG, "Failed automated startup chat restoration: " + e.getMessage());
            }
        }).start();
    }

    private void setupMenuClickListeners() {
        // Toggle slide-out menu drawer
        binding.toolbarMain.setNavigationOnClickListener(v -> {
            WifeLogger.log(TAG, "Navigation menu hamburger button clicked. Toggling side drawer layout.");
            if (binding.drawerLayout.isDrawerOpen(GravityCompat.START)) {
                binding.drawerLayout.closeDrawer(GravityCompat.START);
            } else {
                binding.drawerLayout.openDrawer(GravityCompat.START);
            }
        });

        // Horizontal footer actions
        binding.btnDiscovery.setOnClickListener(v -> {
            WifeLogger.log(TAG, "User launched DeviceDiscoveryActivity.");
            startActivity(new Intent(MainActivity.this, DeviceDiscoveryActivity.class));
        });

        binding.btnTextChat.setOnClickListener(v -> {
            if (connectionManager.isConnected()) {
                WifeLogger.log(TAG, "User launched ChatActivity.");
                Intent intent = new Intent(MainActivity.this, ChatActivity.class);
                // Symmetrical payload forwarding to resolve Glitch 2: Pass current peer device ID
                intent.putExtra(Constants.EXTRA_PEER_MAC, connectionManager.getPeerDeviceId());
                startActivity(intent);
            } else {
                Toast.makeText(this, "Please establish a peer mesh connection first.", Toast.LENGTH_SHORT).show();
            }
        });

        binding.btnDirectCall.setOnClickListener(v -> {
            if (connectionManager.isConnected()) {
                WifeLogger.log(TAG, "User initiated outbound direct voice call from MainActivity.");
                Intent callIntent = new Intent(MainActivity.this, VoiceCallActivity.class);
                callIntent.putExtra("IS_INBOUND", false);
                callIntent.putExtra(Constants.EXTRA_PEER_IP, connectionManager.getPeerIpAddress());
                startActivity(callIntent);
            } else {
                Toast.makeText(this, "Connect to a nearby device to place a call.", Toast.LENGTH_SHORT).show();
            }
        });

        binding.btnVideoCall.setOnClickListener(v -> {
            if (connectionManager.isConnected()) {
                WifeLogger.log(TAG, "User initiated outbound direct video call from MainActivity.");
                Intent callIntent = new Intent(MainActivity.this, VideoCallActivity.class);
                callIntent.putExtra("IS_INBOUND", false);
                callIntent.putExtra(Constants.EXTRA_PEER_IP, connectionManager.getPeerIpAddress());
                startActivity(callIntent);
            } else {
                Toast.makeText(this, "Connect to a nearby device to place a video call.", Toast.LENGTH_SHORT).show();
            }
        });

        // Parallel decoupled 5-way Group Calling Trigger Hook
        binding.btnGroupCall.setOnClickListener(v -> {
            if (connectionManager.isConnected()) {
                WifeLogger.log(TAG, "User launched parallel P2P GroupVideoCallActivity.");
                Intent groupIntent = new Intent(MainActivity.this, GroupVideoCallActivity.class);
                startActivity(groupIntent);
            } else {
                Toast.makeText(this, "Connect to a nearby device to initiate a Group Call.", Toast.LENGTH_SHORT).show();
            }
        });

        binding.btnFileShare.setOnClickListener(v -> {
            if (connectionManager.isConnected()) {
                WifeLogger.log(TAG, "User launched FileTransferActivity.");
                startActivity(new Intent(MainActivity.this, FileTransferActivity.class));
            } else {
                Toast.makeText(this, "Please establish a connection to share files.", Toast.LENGTH_SHORT).show();
            }
        });

        // Slider drawer item actions
        binding.btnMenuMeshLogs.setOnClickListener(v -> {
            binding.drawerLayout.closeDrawer(GravityCompat.START);
            WifeLogger.log(TAG, "User launched ConnectionStatusActivity from drawer.");
            startActivity(new Intent(MainActivity.this, ConnectionStatusActivity.class));
        });

        binding.btnMenuCallHistory.setOnClickListener(v -> {
            binding.drawerLayout.closeDrawer(GravityCompat.START);
            WifeLogger.log(TAG, "User launched CallHistoryActivity from drawer.");
            startActivity(new Intent(MainActivity.this, CallHistoryActivity.class));
        });

        // Additive Drawer Item: Multi-Device Group File Sharing
        binding.btnMenuGroupFileShare.setOnClickListener(v -> {
            binding.drawerLayout.closeDrawer(GravityCompat.START);
            if (connectionManager.isConnected()) {
                WifeLogger.log(TAG, "User launched GroupFileTransferActivity from drawer.");
                startActivity(new Intent(MainActivity.this, GroupFileTransferActivity.class));
            } else {
                Toast.makeText(this, "Please establish a connection to share group files.", Toast.LENGTH_SHORT).show();
            }
        });

        binding.btnMenuSettings.setOnClickListener(v -> {
            binding.drawerLayout.closeDrawer(GravityCompat.START);
            WifeLogger.log(TAG, "User launched SettingsActivity from drawer.");
            startActivity(new Intent(MainActivity.this, SettingsActivity.class));
        });
    }

    @Override
    protected void onResume() {
        super.onResume();
        WifeLogger.log(TAG, "onResume() called. Registering MainActivity to ConnectionManager status listener.");
        connectionManager.registerStatusListener(this);

        // Bind user profile info and unread notification badge from preferences
        try {
            SharedPreferences prefs = getSharedPreferences("WifeSettings", MODE_PRIVATE);
            
            // 1. Update Username Display inside Drawer
            String customName = prefs.getString("custom_alias", Utils.getDeviceModel());
            binding.tvDrawerProfileName.setText(customName);

            // 2. Update Profile Photo inside Drawer
            Bitmap localProfilePhoto = ProfileImageManager.getLocalProfileImage(this);
            if (localProfilePhoto != null) {
                binding.ivDrawerProfilePhoto.setImageBitmap(localProfilePhoto);
            } else {
                binding.ivDrawerProfilePhoto.setImageResource(android.R.drawable.sym_def_app_icon);
            }

            // 3. Update Unread Chat Notification Badge Count
            int unreadCount = prefs.getInt("unread_count", 0);
            WifeLogger.log(TAG, "Evaluating unread notification badge state. Unread Count: " + unreadCount);
            if (unreadCount > 0) {
                binding.tvUnreadBadge.setVisibility(View.VISIBLE);
                binding.tvUnreadBadge.setText(String.valueOf(unreadCount));
            } else {
                binding.tvUnreadBadge.setVisibility(View.GONE);
            }
        } catch (Exception e) {
            WifeLogger.log(TAG, "Failed loading profile or unread badge parameters: " + e.getMessage(), e);
        }
    }

    @Override
    protected void onPause() {
        super.onPause();
        WifeLogger.log(TAG, "onPause() called. Unregistering MainActivity from ConnectionManager status listener.");
        connectionManager.unregisterStatusListener(this);
    }

    @Override
    public void onConnectionStateChanged(boolean connected, String peerIp, boolean isHost) {
        runOnUiThread(() -> {
            if (binding == null) return;
            WifeLogger.log(TAG, "onConnectionStateChanged() triggered. Connected: " + connected + " | Peer IP: " + peerIp + " | Is Host: " + isHost);
            if (connected) {
                binding.vStatusIndicator.setBackgroundResource(android.R.drawable.presence_online);
                binding.tvMainConnectionState.setText("Connected");
                binding.tvMainConnectionState.setTextColor(getResources().getColor(android.R.color.holo_green_dark, getTheme()));
                binding.tvMainPeerDetails.setText("Peer IP: " + peerIp + " | Link Mode: " + (isHost ? "Mesh Host" : "Mesh Client"));
                
                // Start heartbeat keep alive tracking
                HeartbeatManager.getInstance(MainActivity.this).startMonitoring();
            } else {
                binding.vStatusIndicator.setBackgroundResource(android.R.drawable.presence_offline);
                binding.tvMainConnectionState.setText("Disconnected");
                binding.tvMainConnectionState.setTextColor(getResources().getColor(android.R.color.holo_red_dark, getTheme()));
                binding.tvMainPeerDetails.setText("No connected devices nearby.");
                
                HeartbeatManager.getInstance(MainActivity.this).stopMonitoring();
            }
        });
    }
}