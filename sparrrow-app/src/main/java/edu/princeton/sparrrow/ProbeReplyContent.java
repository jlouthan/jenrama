package edu.princeton.sparrrow;

import java.util.UUID;

public class ProbeReplyContent extends MessageContent {
    private final UUID jobID;
    private final int monitorID;

    public ProbeReplyContent(UUID jobID, int monitorID){
        this.jobID = jobID;
        this.monitorID = monitorID;
    }

    public UUID getJobID() {
        return jobID;
    }

    public int getMonitorID() {
        return monitorID;
    }
}
