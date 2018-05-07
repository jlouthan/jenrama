package edu.princeton.sparrrow;

import java.util.UUID;

public class ProbeReplyContent extends MessageContent {
    protected final UUID jobID;
    protected final int monitorID;

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
