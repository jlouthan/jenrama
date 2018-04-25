package edu.princeton.sparrrow;

import java.io.*;
import java.util.ArrayList;

import org.json.JSONObject;


/**
 * The executor receives a job task from a node monitor, executes it, and returns the results.
 */

public class Executor implements Runnable {
    protected final int id;

    // IO streams to and from NodeMonitor
    private PipedInputStream pipeFromMonitor;
    private PipedOutputStream pipeToMonitor;

    private ObjectInputStream objFromMonitor;
    private ObjectOutputStream objToMonitor;

    public Executor(int id, PipedInputStream pipeFromMonitor, PipedOutputStream pipeToMonitor){
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

            // Receive task specification from Scheduler
            taskSpec = (TaskSpecContent)((Message) objFromMonitor.readObject()).getBody();
            // Execute task
            result = execute(taskSpec);
            // Return result
            objToMonitor.writeObject(new Message(MessageType.TASK_RESULT, result));

            // Close IO channels
            pipeFromMonitor.close();
            pipeToMonitor.close();

            log("finishing");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void log(String text){
        System.out.println("Executor: " + text);
    }

    protected TaskResultContent execute(TaskSpecContent s){
        // TODO: figure out how to inherit this class for arbitrary implementations of execute

        log("received task spec from NodeMonitor, beginning execution");
        try {
            for (int i = 0; i < 4; i++) {
                System.out.println("Executor iteration " + i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("Executor thread interrupted.");
        }

        log("finished execution, returning result");
        TaskResultContent myResult = new TaskResultContent(s.getJobID(), s.getTaskID(), s.getSchedID(), "result");

        return myResult;
    }
}
