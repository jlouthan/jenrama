package edu.princeton.sparrrow;

import java.util.UUID;
import org.json.JSONObject;

public class TaskSpecContent extends MessageContent {
    private final UUID jobID;
    private final UUID taskID;
    private final int schedID;
    private final JSONObject spec;

    public TaskSpecContent(UUID jobID, UUID taskID, int schedID, JSONObject spec){
        this.jobID = jobID;
        this.taskID = taskID;
        this.schedID = schedID;
        this.spec = spec;
    }

    public UUID getJobID() {
        return jobID;
    }

    public UUID getTaskID() {
        return taskID;
    }

    public int getSchedID() {
        return schedID;
    }

    public JSONObject getSpec() {
        return spec;
    }
}
