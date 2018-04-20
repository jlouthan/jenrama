package edu.princeton.sparrrow;

import java.util.UUID;

public class ProbeContent extends MessageContent {
    private final UUID jobID;
    private final int schedID;

    public ProbeContent(UUID jobID, int schedID){
        this.jobID = jobID;
        this.schedID = schedID;
    }

    public UUID getJobID() {
        return jobID;
    }

    public int getSchedID() {
        return schedID;
    }
}
