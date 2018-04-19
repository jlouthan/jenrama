package edu.princeton.sparrrow;

import java.util.Collection;
import java.util.UUID;

public class JobSpecContent extends MessageContent {
    UUID jobID;
    int frontendID;
    Collection<JSONObject> tasks;
}
