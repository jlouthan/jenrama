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
        TaskResultContent taskResult;
        taskResult = (TaskResultContent) m;
        // Handle message
        try {
            parent.handleTaskResult(taskResult);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
