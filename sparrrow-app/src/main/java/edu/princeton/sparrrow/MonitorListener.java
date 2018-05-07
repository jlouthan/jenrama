package edu.princeton.sparrrow;

import java.io.IOException;
import java.net.Socket;

public class MonitorListener extends Listener {
    private Scheduler parent;

    public MonitorListener(Socket socketFromMonitor, Scheduler parent) throws IOException {
        super.socketInputStream = socketFromMonitor.getInputStream();
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
