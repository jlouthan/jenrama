package edu.princeton.sparrrow;

import java.io.*;

/**
 * The executor receives a job task from a node monitor, executes it, and returns the results.
 */

public class Executor implements Runnable {
    // IO streams to and from NodeMonitor
    private PipedInputStream pipeFromMonitor;
    private PipedOutputStream pipeToMonitor;

    private ObjectInputStream objFromMonitor;
    private ObjectOutputStream objToMonitor;

    public Executor(PipedInputStream pipeFromMonitor, PipedOutputStream pipeToMonitor){
        this.pipeFromMonitor = pipeFromMonitor;
        this.pipeToMonitor = pipeToMonitor;
    }

    public void run() {
        String taskSpec;
        String result;

        try {
            log("started");

            // Set up object IO with Scheduler
            this.objToMonitor = new ObjectOutputStream(pipeToMonitor);
            this.objFromMonitor = new ObjectInputStream(pipeFromMonitor);

            // Receive task specification from Scheduler
            taskSpec = ((Message) objFromMonitor.readObject()).getBody();
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

    private String execute(String s){
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
        return "Execution result for task: " + s;
    }
}
