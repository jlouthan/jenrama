package edu.princeton.sparrrow;

import java.util.Collection;
import java.util.UUID;
import org.json.JSONObject;

public class JobResultContent extends MessageContent {
    UUID jobID;
    Collection<JSONObject> results;
}
