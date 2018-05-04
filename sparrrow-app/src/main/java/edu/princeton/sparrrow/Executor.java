package edu.princeton.sparrrow;

import java.io.*;
import java.util.ArrayList;

import org.json.JSONObject;


/**
 * The executor receives a job task from a node monitor, executes it, and returns the results.
 */

public abstract class Executor implements Runnable {
    protected final int id;

    // IO streams to and from NodeMonitor
    private PipedInputStream pipeFromMonitor;
    private PipedOutputStream pipeToMonitor;

    private ObjectInputStream objFromMonitor;
    private ObjectOutputStream objToMonitor;

    protected Executor(int id, PipedInputStream pipeFromMonitor, PipedOutputStream pipeToMonitor){
        this.id = id;
        this.pipeFromMonitor = pipeFromMonitor;
        this.pipeToMonitor = pipeToMonitor;
    }

    public void run() {
        TaskSpecContent taskSpec;
        TaskResultContent result;

        try {
            log("started");

            // Set up object IO with Scheduler
            this.objToMonitor = new ObjectOutputStream(pipeToMonitor);
            this.objFromMonitor = new ObjectInputStream(pipeFromMonitor);

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
                pipeFromMonitor.close();
                pipeToMonitor.close();
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
