package edu.princeton.sparrrow;

import java.util.Collection;
import java.util.UUID;
import org.json.JSONObject;

public class JobResultContent extends MessageContent {
    private final UUID jobID;
    private final Collection<String> results;

    public JobResultContent(UUID jobID, Collection<String> results){
        this.jobID = jobID;
        this.results = results;
    }

    public UUID getJobID() {
        return jobID;
    }

    public Collection<String> getResults() {
        return results;
    }
}
