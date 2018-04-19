package edu.princeton.sparrrow;

import java.util.UUID;

public class JobResultContent extends MessageContent {
    UUID jobID;
    Collection<JSONObject> results;
}
