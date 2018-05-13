package edu.princeton.sparrrow;


import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class ExecutorListener extends Listener {
    private NodeMonitor parent;

    public ExecutorListener(InputStream socketFromExecutor, NodeMonitor parent) throws IOException {
        super.objInputStream = new ObjectInputStream(socketFromExecutor);
        this.parent = parent;
        super.parent = parent;
    }

    public void handleMessage(MessageContent m) {
        try {
            if (m instanceof TaskResultContent) {
                parent.handleTaskResult( (TaskResultContent) m);
            } else if (m instanceof DoneAckContent){
                parent.handleDoneAck( (DoneAckContent) m);
                done = true;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
