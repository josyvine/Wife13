package com.wife.app;

import android.app.AlertDialog;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.provider.OpenableColumns;
import android.view.View;
import android.widget.ProgressBar;
import android.widget.TextView;
import android.widget.Toast;

import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.appcompat.app.AppCompatActivity;
import androidx.localbroadcastmanager.content.LocalBroadcastManager;
import androidx.recyclerview.widget.LinearLayoutManager;

import com.wife.app.databinding.ActivityGroupFileTransferBinding;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupFileTransferActivity extends AppCompatActivity implements 
        GroupFileReceiver.GroupFileReceiveListener,
        FileAdapter.OnFileDeleteListener {

    private static final String TAG = "GroupFileTransferActivity";

    private ActivityGroupFileTransferBinding binding;
    private FileAdapter adapter;
    private PeerSelectAdapter peerAdapter;

    private final List<FileEntity> historyList = new ArrayList<>();
    private final List<PeerSelectAdapter.PeerItem> peerList = new ArrayList<>();
    private RoomDatabaseManager db;

    // High-performance cache map to avoid slow recursive tree traversals on target progress rows
    private final Map<String, View> activePeerProgressViews = new HashMap<>();

    // Additive multi-file picker contract to retrieve and process targeted contents
    private final ActivityResultLauncher<String> filePickerLauncher = registerForActivityResult(
            new ActivityResultContracts.GetMultipleContents(),
            this::onFilesSelected
    );

    // --- High-Speed Real-time Broadcast Receiver for Parallel Group Transfers ---
    private final BroadcastReceiver groupTransferReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            String action = intent.getAction();
            if (action == null) return;

            switch (action) {
                case Constants.ACTION_GROUP_TRANSFER_PROGRESS:
                    String filename = intent.getStringExtra(Constants.EXTRA_FILE_NAME);
                    long transferred = intent.getLongExtra(Constants.EXTRA_BYTES_TRANSFERRED, 0);
                    long total = intent.getLongExtra(Constants.EXTRA_TOTAL_BYTES, 0);
                    int percent = (total > 0) ? (int) ((transferred * 100) / total) : 0;
                    double speed = intent.getDoubleExtra(Constants.EXTRA_TRANSFER_SPEED, 0.0);
                    String peerIp = intent.getStringExtra(Constants.EXTRA_PEER_IP);

                    binding.layoutTransferProgress.setVisibility(View.VISIBLE);

                    if (peerIp != null && !peerIp.isEmpty()) {
                        // Locate or dynamically inflate progress trackers for this specific target IP
                        View progressRow = activePeerProgressViews.get(peerIp);
                        if (progressRow == null) {
                            progressRow = getLayoutInflater().inflate(R.xml.item_group_progress, binding.containerActivePeersProgress, false);
                            binding.containerActivePeersProgress.addView(progressRow);
                            activePeerProgressViews.put(peerIp, progressRow);
                        }

                        TextView tvLabel = progressRow.findViewById(R.id.tvGroupPeerLabel);
                        ProgressBar pbBar = progressRow.findViewById(R.id.pbGroupPeerPercentage);
                        TextView tvStats = progressRow.findViewById(R.id.tvGroupPeerStats);
                        TextView tvPercent = progressRow.findViewById(R.id.tvGroupPeerPercentText);

                        pbBar.setProgress(percent);
                        tvPercent.setText(percent + "%");

                        String speedStr = String.format(java.util.Locale.US, "%.1f MB/s", speed);
                        String sizeStr = Utils.formatFileSize(transferred) + " / " + Utils.formatFileSize(total);

                        // Extract peer custom name if available in our adapter cache
                        String displayName = peerIp;
                        for (PeerSelectAdapter.PeerItem item : peerList) {
                            if (peerIp.equals(item.getIpAddress())) {
                                displayName = item.getDeviceName();
                                break;
                            }
                        }

                        tvLabel.setText(displayName);
                        if (filename != null && filename.startsWith("Compressing:")) {
                            tvStats.setText(sizeStr + " (Compressing...)");
                        } else {
                            tvStats.setText(sizeStr + " (" + speedStr + ")");
                        }
                    }

                    // Keep aggregate progress state updated on the main card view
                    binding.tvActiveFileName.setText("Processing targeted multi-device queue...");
                    binding.pbTransferPercentage.setProgress(percent);
                    binding.tvTransferPercentText.setText(percent + "%");
                    break;

                case Constants.ACTION_GROUP_TRANSFER_COMPLETE:
                    String completeIp = intent.getStringExtra(Constants.EXTRA_PEER_IP);
                    if (completeIp != null) {
                        View row = activePeerProgressViews.remove(completeIp);
                        if (row != null) {
                            binding.containerActivePeersProgress.removeView(row);
                        }
                    }

                    // Complete cleanup once all parallel queues resolve
                    if (activePeerProgressViews.isEmpty()) {
                        Toast.makeText(GroupFileTransferActivity.this, "Group transfer completed successfully!", Toast.LENGTH_SHORT).show();
                        binding.layoutTransferProgress.setVisibility(View.GONE);
                    }
                    loadHistory();
                    break;

                case Constants.ACTION_GROUP_TRANSFER_ERROR:
                    String error = intent.getStringExtra(Constants.EXTRA_ERROR_MESSAGE);
                    String errorIp = intent.getStringExtra(Constants.EXTRA_PEER_IP);
                    
                    if (errorIp != null) {
                        View row = activePeerProgressViews.remove(errorIp);
                        if (row != null) {
                            binding.containerActivePeersProgress.removeView(row);
                        }
                    }

                    if (activePeerProgressViews.isEmpty()) {
                        if ("Group transfer cancelled by user.".equals(error)) {
                            Toast.makeText(GroupFileTransferActivity.this, "Group transfer cancelled.", Toast.LENGTH_SHORT).show();
                        } else {
                            Toast.makeText(GroupFileTransferActivity.this, "Group transfer completed with alerts.", Toast.LENGTH_SHORT).show();
                        }
                        binding.layoutTransferProgress.setVisibility(View.GONE);
                    }
                    loadHistory();
                    break;
            }
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = ActivityGroupFileTransferBinding.inflate(getLayoutInflater());
        setContentView(binding.getRoot());

        db = RoomDatabaseManager.getInstance(this);

        setupToolbar();
        setupRecyclerView();
        setupPeerList();

        binding.btnPickFile.setOnClickListener(v -> {
            // Enforce select roster validation checks before opening file system picker
            List<String> selectedIps = getSelectedPeerIps();
            if (selectedIps.isEmpty()) {
                Toast.makeText(this, "Please select at least one recipient device first.", Toast.LENGTH_SHORT).show();
                return;
            }
            filePickerLauncher.launch("*/*");
        });

        binding.layoutTransferProgress.setOnClickListener(v -> showTransferOptionsDialog());

        loadHistory();
    }

    private void setupToolbar() {
        setSupportActionBar(binding.toolbarFileTransfer);
        if (getSupportActionBar() != null) {
            getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        }
        binding.toolbarFileTransfer.setNavigationOnClickListener(v -> onBackPressed());
    }

    private void setupRecyclerView() {
        adapter = new FileAdapter(historyList, this);
        binding.rvFileHistory.setLayoutManager(new LinearLayoutManager(this));
        binding.rvFileHistory.setAdapter(adapter);
    }

    private void setupPeerList() {
        peerAdapter = new PeerSelectAdapter(peerList);
        binding.rvPeerSelect.setLayoutManager(new LinearLayoutManager(this, LinearLayoutManager.HORIZONTAL, false));
        binding.rvPeerSelect.setAdapter(peerAdapter);
    }

    private void populateOnlinePeers() {
        peerList.clear();
        java.util.Map<String, String> activePeers = ConnectionManager.getInstance(this).getGroupPeers();
        for (Map.Entry<String, String> entry : activePeers.entrySet()) {
            // Attempt to resolve custom alias, defaulting to device hardware model
            String displayName = entry.getKey();
            if (displayName.startsWith("peer_")) {
                displayName = "Mesh Client (" + entry.getValue() + ")";
            }
            peerList.add(new PeerSelectAdapter.PeerItem(entry.getKey(), displayName, entry.getValue()));
        }
        
        if (peerList.isEmpty()) {
            binding.tvNoPeersDetected.setVisibility(View.VISIBLE);
            binding.rvPeerSelect.setVisibility(View.GONE);
        } else {
            binding.tvNoPeersDetected.setVisibility(View.GONE);
            binding.rvPeerSelect.setVisibility(View.VISIBLE);
        }
        peerAdapter.notifyDataSetChanged();
    }

    private List<String> getSelectedPeerIps() {
        List<String> selected = new ArrayList<>();
        for (PeerSelectAdapter.PeerItem item : peerList) {
            if (item.isSelected()) {
                selected.add(item.getIpAddress());
            }
        }
        return selected;
    }

    private void loadHistory() {
        new Thread(() -> {
            try {
                List<FileEntity> logs = db.fileDao().getAllFiles();
                runOnUiThread(() -> {
                    try {
                        historyList.clear();
                        historyList.addAll(logs);
                        adapter.notifyDataSetChanged();
                    } catch (Exception e) {
                        WifeLogger.log(TAG, "Failed updating adapter list: " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                WifeLogger.log(TAG, "Database read thread exception: " + e.getMessage());
            }
        }).start();
    }

    private void onFilesSelected(List<Uri> uris) {
        if (uris == null || uris.isEmpty()) return;

        List<String> targetIps = getSelectedPeerIps();
        if (targetIps.isEmpty()) {
            Toast.makeText(this, "Roster selection cancelled.", Toast.LENGTH_SHORT).show();
            return;
        }

        ArrayList<String> uriStrings = new ArrayList<>();
        ArrayList<String> fileNames = new ArrayList<>();
        long[] fileSizes = new long[uris.size()];

        for (int i = 0; i < uris.size(); i++) {
            Uri uri = uris.get(i);
            uriStrings.add(uri.toString());

            String filename = "Group_File_" + i;
            long size = 0;

            try (Cursor cursor = getContentResolver().query(uri, null, null, null, null)) {
                if (cursor != null && cursor.moveToFirst()) {
                    int nameIdx = cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME);
                    int sizeIdx = cursor.getColumnIndex(OpenableColumns.SIZE);
                    
                    if (nameIdx != -1) filename = cursor.getString(nameIdx);
                    if (sizeIdx != -1) size = cursor.getLong(sizeIdx);
                }
            } catch (Exception e) {
                WifeLogger.log(TAG, "Cursor query failed for file index: " + i);
            }
            fileNames.add(filename);
            fileSizes[i] = size;
        }

        binding.layoutTransferProgress.setVisibility(View.VISIBLE);
        binding.containerActivePeersProgress.removeAllViews();
        activePeerProgressViews.clear();

        if (uris.size() == 1) {
            binding.tvActiveFileName.setText("Uploading: " + fileNames.get(0));
        } else {
            binding.tvActiveFileName.setText("Uploading " + uris.size() + " files...");
        }
        binding.pbTransferPercentage.setProgress(0);
        binding.tvTransferPercentText.setText("0%");
        binding.tvTransferSpeedAndSize.setText("");

        // Launch the additive Group Foreground service with designated targets
        Intent serviceIntent = new Intent(this, GroupFileTransferForegroundService.class);
        serviceIntent.setAction(Constants.ACTION_GROUP_START_TRANSFER);
        serviceIntent.putExtra("IS_SENDER", true);
        serviceIntent.putStringArrayListExtra("URI_LIST", uriStrings);
        serviceIntent.putStringArrayListExtra("FILE_NAMES", fileNames);
        serviceIntent.putExtra("FILE_SIZES", fileSizes);
        serviceIntent.putStringArrayListExtra("TARGET_IPS", new ArrayList<>(targetIps));
        startService(serviceIntent);
    }

    private void showTransferOptionsDialog() {
        String[] options = GroupFileTransferForegroundService.isPaused ? 
                new String[]{"Resume Transfer", "Cancel Transfer"} : 
                new String[]{"Pause Transfer", "Cancel Transfer"};

        new AlertDialog.Builder(this)
                .setTitle("Group Transfer Controls")
                .setItems(options, (dialog, which) -> {
                    Intent intent = new Intent(this, GroupFileTransferForegroundService.class);
                    if (which == 0) {
                        if (GroupFileTransferForegroundService.isPaused) {
                            intent.setAction(Constants.ACTION_GROUP_RESUME_TRANSFER);
                            Toast.makeText(this, "Resuming...", Toast.LENGTH_SHORT).show();
                        } else {
                            intent.setAction(Constants.ACTION_GROUP_PAUSE_TRANSFER);
                            Toast.makeText(this, "Pausing...", Toast.LENGTH_SHORT).show();
                        }
                    } else {
                        intent.setAction(Constants.ACTION_GROUP_CANCEL_TRANSFER);
                        Toast.makeText(this, "Cancelling...", Toast.LENGTH_SHORT).show();
                    }
                    startService(intent);
                })
                .show();
    }

    @Override
    public void onFileDelete(FileEntity file, int position) {
        WifeLogger.log(TAG, "User requested log purge for: " + file.getFilename());
        new Thread(() -> {
            try {
                db.fileDao().deleteById(file.getId());
                runOnUiThread(() -> {
                    try {
                        if (position < historyList.size()) {
                            historyList.remove(position);
                            adapter.notifyItemRemoved(position);
                            adapter.notifyItemRangeChanged(position, historyList.size());
                            Toast.makeText(this, "Purged from log.", Toast.LENGTH_SHORT).show();
                        }
                    } catch (Exception e) {
                        WifeLogger.log(TAG, "UI list sync exception: " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                WifeLogger.log(TAG, "History deletion task failed: " + e.getMessage());
            }
        }).start();
    }

    // --- Dynamic Listener callbacks delegated from Group Receiver ---

    @Override
    public void onProgress(String filename, int percent) {
        // Compatibility Interface signature compatibility
    }

    @Override
    public void onComplete(String filename, String localPath) {
        WifeLogger.log(TAG, "onComplete received for group file: " + filename + " | LocalPath: " + localPath);
        runOnUiThread(this::loadHistory);
    }

    @Override
    public void onError(String error) {
        // Optional receiver error handling
    }

    @Override
    protected void onResume() {
        super.onResume();
        GroupFileReceiver.registerListener(this);
        populateOnlinePeers();

        IntentFilter filter = new IntentFilter();
        filter.addAction(Constants.ACTION_GROUP_TRANSFER_PROGRESS);
        filter.addAction(Constants.ACTION_GROUP_TRANSFER_COMPLETE);
        filter.addAction(Constants.ACTION_GROUP_TRANSFER_ERROR);
        LocalBroadcastManager.getInstance(this).registerReceiver(groupTransferReceiver, filter);
    }

    @Override
    protected void onPause() {
        super.onPause();
        GroupFileReceiver.unregisterListener(this);
        LocalBroadcastManager.getInstance(this).unregisterReceiver(groupTransferReceiver);
    }
}