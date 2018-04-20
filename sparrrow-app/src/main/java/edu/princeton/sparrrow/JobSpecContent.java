package edu.princeton.sparrrow;

import java.util.Collection;
import java.util.UUID;
import org.json.JSONObject;

public class JobSpecContent extends MessageContent {
    UUID jobID;
    int frontendID;
    Collection<JSONObject> tasks;
}
