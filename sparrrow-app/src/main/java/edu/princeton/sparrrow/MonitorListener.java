package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PipedInputStream;

public class MonitorListener extends Listener {
    private Scheduler parent;

    public MonitorListener(PipedInputStream pipeFromMonitor, Scheduler parent) throws IOException {
        super.pipeInputStream = pipeFromMonitor;
        this.parent = parent;
    }

    public void handleMessage(MessageContent m){
        TaskResultContent taskResult;
        ProbeReplyContent probeReply;

        try {
            if (m instanceof ProbeReplyContent) {
                // Receive probe from scheduler
                probeReply = (ProbeReplyContent) m;
                // Handle message
                    parent.receivedSpecRequest(probeReply);

            } else {
                // Receive task specification from Scheduler
                taskResult = (TaskResultContent) m;
                // Handle message
                parent.receivedResult(taskResult);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
