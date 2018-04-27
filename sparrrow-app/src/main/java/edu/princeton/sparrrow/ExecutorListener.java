package edu.princeton.sparrrow;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PipedInputStream;

public class ExecutorListener extends Listener {
    private NodeMonitor parent;

    public ExecutorListener(PipedInputStream pipeFromExecutor, NodeMonitor parent) throws IOException {
        super.inputStream = new ObjectInputStream(pipeFromExecutor);
        this.parent = parent;
    }

    public void handleMessage(MessageContent m) {
        TaskResultContent taskResult;
        taskResult = (TaskResultContent) m;
        // Handle message
        try {
            parent.receivedResult(taskResult);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
