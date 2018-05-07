package edu.princeton.sparrrow;

import java.util.UUID;

public class TaskResultContent extends MessageContent {
    private final UUID jobID;
    private final UUID taskID;
    private final int schedID;
    private final String result;

    public TaskResultContent(UUID jobID, UUID taskID, int schedID, String result){
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

    public String getResult() {
        return result;
    }
}
