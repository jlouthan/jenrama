package edu.princeton.sparrrow;

import java.util.UUID;

public class DetailedProbeReplyContent extends ProbeReplyContent {
    private final int qLength;

    public DetailedProbeReplyContent(UUID jobID, int monitorID, int qLength){
        super(jobID, monitorID);
        this.qLength = qLength;
    }

    public UUID getJobID() {
        return jobID;
    }

    public int getMonitorID() {
        return monitorID;
    }

    public int getqLength() { return qLength; };
}
