package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class FrontendListener extends Listener {
    private Scheduler parent;

    public FrontendListener (InputStream socketFromFrontend, Scheduler parent) throws IOException {
        super.objInputStream = new ObjectInputStream(socketFromFrontend);
        this.parent = parent;
    }

    public void handleMessage(MessageContent m) {
        JobSpecContent newJob;
        try {
            newJob = (JobSpecContent)m;
            parent.receivedJob(newJob);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}