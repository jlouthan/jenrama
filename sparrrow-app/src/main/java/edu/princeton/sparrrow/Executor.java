package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * The executor receives a job task from a node monitor, executes it, and returns the results.
 */

public abstract class Executor implements Runnable, Logger {
    protected final int id;
    private boolean done = false;

    // IO streams to and from NodeMonitor
    private Socket socketWithMonitor;

    private ObjectInputStream objFromMonitor;
    private ObjectOutputStream objToMonitor;

    protected Executor(int id, ServerSocket socketWithMonitor) throws IOException {
        this.id = id;
        this.socketWithMonitor = socketWithMonitor.accept();
    }

    public void run() {
        MessageContent incomingContent;
        TaskSpecContent taskSpec;
        TaskResultContent result;

        try {
            log("started");

            // Set up object IO with Scheduler
            this.objToMonitor = new ObjectOutputStream(socketWithMonitor.getOutputStream());
            this.objFromMonitor = new ObjectInputStream(socketWithMonitor.getInputStream());

            while(!done) {
                incomingContent = ((Message) objFromMonitor.readObject()).getBody();

                if (incomingContent instanceof TaskSpecContent){
                    // Receive task specification from Scheduler
                    taskSpec = (TaskSpecContent) incomingContent;
                    // Execute task
                    log("executing task " + taskSpec.getTaskID());
                    result = execute(taskSpec);
                    // Return result
                    objToMonitor.writeObject(new Message(MessageType.TASK_RESULT, result));
                } else if (incomingContent instanceof DoneContent){
                    DoneAckContent ack = new DoneAckContent(id);
                    objToMonitor.writeObject(new Message(MessageType.DONE_ACK, ack));
                    done = true;
                }
            }
            socketWithMonitor.close();
            log("finishing");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized void log(String text){
        System.out.println("Executor[" + this.id + "]: " + text);
    }

    protected abstract TaskResultContent execute(TaskSpecContent s);
}
