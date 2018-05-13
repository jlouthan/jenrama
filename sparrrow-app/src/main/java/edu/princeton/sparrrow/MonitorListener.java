package edu.princeton.sparrrow;

import java.io.IOException;
import java.net.Socket;

public class MonitorListener extends Listener {
    private Scheduler parent;

    public MonitorListener(Socket socketFromMonitor, Scheduler parent) throws IOException {
        super.socketInputStream = socketFromMonitor.getInputStream();
        this.parent = parent;
        super.parent = parent;
    }

    public void handleMessage(MessageContent m){
        TaskResultContent taskResult;
        ProbeReplyContent probeReply;

        try {
            if (m instanceof ProbeReplyContent) {
                // Receive probe reply from monitor
                probeReply = (ProbeReplyContent) m;
                // Handle message
                parent.receivedSpecRequest(probeReply);
            } else if (m instanceof TaskResultContent) {
                // Receive task result from monitor
                taskResult = (TaskResultContent) m;
                // Handle message
                parent.receivedResult(taskResult);
            } else {
                parent.log("recieved unexpected message type from Monitor: " + m.getClass());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
