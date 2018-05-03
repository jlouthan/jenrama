package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PipedInputStream;

public class FrontendListener extends Listener {
    private Scheduler parent;

    public FrontendListener (PipedInputStream pipeFromFrontend, Scheduler parent) throws IOException {
        super.inputStream = new ObjectInputStream(pipeFromFrontend);
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
