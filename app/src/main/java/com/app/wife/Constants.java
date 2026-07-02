package com.wife.app;

public final class Constants {
    private Constants() {}

    // Socket ports on the devices
    public static final int OFF_PORT_CONTROL = 8888;
    public static final int OFF_PORT_TEXT = 8889;
    public static final int OFF_PORT_FILE = 8900;
    public static final int OFF_PORT_VOICE = 8901;
    public static final int OFF_PORT_VIDEO = 8902;
    
    // Decoupled group call UDP ports to maintain parallel pipelines
    public static final int OFF_PORT_GROUP_AUDIO = 8903;
    public static final int OFF_PORT_GROUP_VIDEO = 8904;

    // Decoupled group file transfer TCP port
    public static final int OFF_PORT_GROUP_FILE = 8905;

    // Intents and extras keys
    public static final String EXTRA_PEER_IP = "com.wife.app.EXTRA_PEER_IP";
    public static final String EXTRA_PEER_NAME = "com.wife.app.EXTRA_PEER_NAME";
    public static final String EXTRA_PEER_MAC = "com.wife.app.EXTRA_PEER_MAC";
    public static final String EXTRA_IS_HOST = "com.wife.app.EXTRA_IS_HOST";

    // Call Signaling events
    public static final String SIGNAL_CALL_REQUEST = "CALL_REQUEST";
    public static final String SIGNAL_CALL_ACCEPT = "CALL_ACCEPT";
    public static final String SIGNAL_CALL_REJECT = "CALL_REJECT";
    public static final String SIGNAL_CALL_END = "CALL_END";

    public static final String SIGNAL_VIDEO_REQUEST = "VIDEO_CALL_REQUEST";
    public static final String SIGNAL_VIDEO_ACCEPT = "VIDEO_CALL_ACCEPT";
    public static final String SIGNAL_VIDEO_REJECT = "VIDEO_CALL_REJECT";
    public static final String SIGNAL_VIDEO_END = "VIDEO_CALL_END";

    // Decoupled group call signaling events
    public static final String SIGNAL_GROUP_CALL_REQUEST = "GROUP_CALL_REQUEST";
    public static final String SIGNAL_GROUP_CALL_ACCEPT = "GROUP_CALL_ACCEPT";
    public static final String SIGNAL_GROUP_CALL_REJECT = "GROUP_CALL_REJECT";
    public static final String SIGNAL_GROUP_CALL_END = "GROUP_CALL_END";

    // Peer Roster synchronization signal for multi-device transfers
    public static final String SIGNAL_PEER_ROSTER_SYNC = "PEER_ROSTER_SYNC";

    // Broadcast actions
    public static final String ACTION_CONNECTION_CHANGED = "com.wife.app.ACTION_CONNECTION_CHANGED";
    public static final String ACTION_PEER_DISCOVERED = "com.wife.app.ACTION_PEER_DISCOVERED";
    public static final String ACTION_CALL_RECEIVED = "com.wife.app.ACTION_CALL_RECEIVED";
    public static final String ACTION_VIDEO_CALL_RECEIVED = "com.wife.app.ACTION_VIDEO_CALL_RECEIVED";
    
    // Decoupled group call broadcast action
    public static final String ACTION_GROUP_CALL_RECEIVED = "com.wife.app.ACTION_GROUP_CALL_RECEIVED";
    
    public static final String EXTRA_CALL_SENDER = "EXTRA_CALL_SENDER";
    public static final String EXTRA_CALL_IP = "EXTRA_CALL_IP";

    // --- File Transfer Actions ---
    public static final String ACTION_START_TRANSFER = "com.wife.app.ACTION_START_TRANSFER";
    public static final String ACTION_PAUSE_TRANSFER = "com.wife.app.ACTION_PAUSE_TRANSFER";
    public static final String ACTION_RESUME_TRANSFER = "com.wife.app.ACTION_RESUME_TRANSFER";
    public static final String ACTION_CANCEL_TRANSFER = "com.wife.app.ACTION_CANCEL_TRANSFER";
    
    // --- File Transfer Progress Broadcast Actions ---
    public static final String ACTION_TRANSFER_PROGRESS = "com.wife.app.ACTION_TRANSFER_PROGRESS";
    public static final String ACTION_TRANSFER_COMPLETE = "com.wife.app.ACTION_TRANSFER_COMPLETE";
    public static final String ACTION_TRANSFER_ERROR = "com.wife.app.ACTION_TRANSFER_ERROR";

    // --- Decoupled Group File Transfer Actions ---
    public static final String ACTION_GROUP_START_TRANSFER = "com.wife.app.ACTION_GROUP_START_TRANSFER";
    public static final String ACTION_GROUP_PAUSE_TRANSFER = "com.wife.app.ACTION_GROUP_PAUSE_TRANSFER";
    public static final String ACTION_GROUP_RESUME_TRANSFER = "com.wife.app.ACTION_GROUP_RESUME_TRANSFER";
    public static final String ACTION_GROUP_CANCEL_TRANSFER = "com.wife.app.ACTION_GROUP_CANCEL_TRANSFER";

    // --- Decoupled Group File Transfer Progress Broadcast Actions ---
    public static final String ACTION_GROUP_TRANSFER_PROGRESS = "com.wife.app.ACTION_GROUP_TRANSFER_PROGRESS";
    public static final String ACTION_GROUP_TRANSFER_COMPLETE = "com.wife.app.ACTION_GROUP_TRANSFER_COMPLETE";
    public static final String ACTION_GROUP_TRANSFER_ERROR = "com.wife.app.ACTION_GROUP_TRANSFER_ERROR";

    // --- File Transfer Progress Extras Keys ---
    public static final String EXTRA_BYTES_TRANSFERRED = "com.wife.app.EXTRA_BYTES_TRANSFERRED";
    public static final String EXTRA_TOTAL_BYTES = "com.wife.app.EXTRA_TOTAL_BYTES";
    public static final String EXTRA_FILE_NAME = "com.wife.app.EXTRA_FILE_NAME";
    public static final String EXTRA_TRANSFER_SPEED = "com.wife.app.EXTRA_TRANSFER_SPEED";
    public static final String EXTRA_FILE_INDEX = "com.wife.app.EXTRA_FILE_INDEX";
    public static final String EXTRA_ERROR_MESSAGE = "com.wife.app.EXTRA_ERROR_MESSAGE";
}