package edu.princeton.sparrrow;

import java.util.Collection;
import java.util.UUID;
import org.json.JSONObject;

public class JobSpecContent extends MessageContent {
    private final UUID jobID;
    private final int frontendID;
    private final Collection<JSONObject> tasks;

    public JobSpecContent(UUID jobID, int frontendID, Collection<JSONObject> tasks){
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

    public Collection<JSONObject> getTasks() {
        return tasks;
    }
}
