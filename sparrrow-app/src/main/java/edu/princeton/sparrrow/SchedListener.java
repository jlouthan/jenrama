package edu.princeton.sparrrow;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PipedInputStream;

public class SchedListener extends Listener {
    private NodeMonitor parent;

    public SchedListener(PipedInputStream pipeFromSched, NodeMonitor parent) throws IOException {
        super.inputStream = new ObjectInputStream(pipeFromSched);
        this.parent = parent;
    }

    public void handleMessage(MessageContent m) {
        ProbeContent probe;
        TaskSpecContent taskSpec;

        try {
            if (m instanceof ProbeContent) {
                // Receive probe from scheduler
                probe = (ProbeContent) m;
                // Handle message
                parent.handleProbe(probe);
            } else if (m instanceof TaskSpecContent){
                // Receive task specification from Scheduler
                taskSpec = (TaskSpecContent) m;
                // Handle message
                parent.handleTaskSpec(taskSpec);
            } else {
                parent.log("ERROR: received message with wrong type");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
