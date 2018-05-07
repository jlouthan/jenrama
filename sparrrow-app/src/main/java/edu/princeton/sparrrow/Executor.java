package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * The executor receives a job task from a node monitor, executes it, and returns the results.
 */

public abstract class Executor implements Runnable {
    protected final int id;

    // IO streams to and from NodeMonitor
    private Socket socketWithMonitor;

    private ObjectInputStream objFromMonitor;
    private ObjectOutputStream objToMonitor;

    protected Executor(int id, ServerSocket socketWithMonitor) throws IOException {
        this.id = id;
        this.socketWithMonitor = socketWithMonitor.accept();
    }

    public void run() {
        TaskSpecContent taskSpec;
        TaskResultContent result;

        try {
            log("started");

            // Set up object IO with Scheduler
            this.objToMonitor = new ObjectOutputStream(socketWithMonitor.getOutputStream());
            this.objFromMonitor = new ObjectInputStream(socketWithMonitor.getInputStream());

            while(true) {
                // Receive task specification from Scheduler
                taskSpec = (TaskSpecContent) ((Message) objFromMonitor.readObject()).getBody();
                // Execute task
                log("executing task " + taskSpec.getTaskID());
                result = execute(taskSpec);
                // Return result
                objToMonitor.writeObject(new Message(MessageType.TASK_RESULT, result));

                // Close IO channels
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                socketWithMonitor.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            log("finishing");
        }
    }

    private void log(String text){
        System.out.println("Executor[" + this.id + "]: " + text);
    }

    protected abstract TaskResultContent execute(TaskSpecContent s);
}
