package edu.princeton.sparrrow;

import java.util.UUID;
import org.json.JSONObject;

public class TaskResultContent extends MessageContent {
    private final UUID jobID;
    private final UUID taskID;
    private final int schedID;
    private final JSONObject result;

    public TaskResultContent(UUID jobID, UUID taskID, int schedID, JSONObject result){
        this.jobID = jobID;
        this.taskID = taskID;
        this.schedID = schedID;
        this.result = result;
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

    public JSONObject getResult() {
        return result;
    }
}
