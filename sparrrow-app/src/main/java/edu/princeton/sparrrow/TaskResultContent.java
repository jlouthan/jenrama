package edu.princeton.sparrrow;

import java.util.UUID;

public class TaskResultContent extends MessageContent {
    UUID jobID;
    UUID taskID;
    int schedID;
    JSONObject result;
}
