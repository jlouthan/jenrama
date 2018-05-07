package edu.princeton.sparrrow;

import java.util.Collection;
import java.util.UUID;

public class JobSpecContent extends MessageContent {
    private final UUID jobID;
    private final int frontendID;
    private final Collection<String> tasks;

    public JobSpecContent(UUID jobID, int frontendID, Collection<String> tasks){
        this.jobID = jobID;
        this.frontendID = frontendID;
        this.tasks = tasks;
    }

    public UUID getJobID() {
        return jobID;
    }

    public int getFrontendID() {
        return frontendID;
    }

    public Collection<String> getTasks() {
        return tasks;
    }
}
