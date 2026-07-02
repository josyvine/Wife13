package com.wife.app;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.CheckBox;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import java.util.List;

public class PeerSelectAdapter extends RecyclerView.Adapter<PeerSelectAdapter.ViewHolder> {

    private final List<PeerItem> peers;

    public static class PeerItem {
        private final String id;
        private final String deviceName;
        private final String ipAddress;
        private boolean selected;

        public PeerItem(String id, String deviceName, String ipAddress) {
            this.id = id;
            this.deviceName = deviceName;
            this.ipAddress = ipAddress;
            this.selected = false; // Default unchecked on discovery
        }

        public String getId() {
            return id;
        }

        public String getDeviceName() {
            return deviceName;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public boolean isSelected() {
            return selected;
        }

        public void setSelected(boolean selected) {
            this.selected = selected;
        }
    }

    public PeerSelectAdapter(List<PeerItem> peers) {
        this.peers = peers;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.item_peer_select, parent, false);
        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        final PeerItem item = peers.get(position);
        
        holder.tvName.setText(item.getDeviceName());
        holder.cbSelect.setChecked(item.isSelected());

        // Standard profile image fallback
        holder.ivAvatar.setImageResource(android.R.drawable.sym_def_app_icon);

        // Allow toggling of checkbox selection state on entire card tap
        holder.itemView.setOnClickListener(v -> {
            boolean nextState = !item.isSelected();
            item.setSelected(nextState);
            holder.cbSelect.setChecked(nextState);
        });

        // Ensure direct checkbox click is also captured and saved to selection state
        holder.cbSelect.setOnClickListener(v -> {
            item.setSelected(holder.cbSelect.isChecked());
        });
    }

    @Override
    public int getItemCount() {
        return peers.size();
    }

    public static class ViewHolder extends RecyclerView.ViewHolder {
        ImageView ivAvatar;
        TextView tvName;
        CheckBox cbSelect;

        public ViewHolder(@NonNull View itemView) {
            super(itemView);
            ivAvatar = itemView.findViewById(R.id.ivPeerAvatar);
            tvName = itemView.findViewById(R.id.tvPeerName);
            cbSelect = itemView.findViewById(R.id.cbPeerSelect);
        }
    }
}