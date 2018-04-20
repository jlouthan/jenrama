package edu.princeton.sparrrow;

import java.util.UUID;
import org.json.JSONObject;

public class TaskSpecContent extends MessageContent {
    UUID jobID;
    UUID taskID;
    int schedID;
    JSONObject spec;
}
