package edu.princeton.sparrrow;

import java.util.UUID;
import org.json.JSONObject;

public class TaskResultContent extends MessageContent {
    UUID jobID;
    UUID taskID;
    int schedID;
    JSONObject result;
}
