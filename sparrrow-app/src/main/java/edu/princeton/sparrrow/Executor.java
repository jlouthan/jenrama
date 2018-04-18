package edu.princeton.sparrrow;

import java.io.*;

/**
 * The executor receives a job task from a node monitor, executes it, and returns the results.
 */

public class Executor implements Runnable {
    // IO streams to and from NodeMonitor
    private PipedInputStream pipe_from_monitor;
    private PipedOutputStream pipe_to_monitor;

    private ObjectInputStream obj_from_monitor;
    private ObjectOutputStream obj_to_monitor;

    public Executor(PipedInputStream pipe_from_monitor, PipedOutputStream pipe_to_monitor){
        this.pipe_from_monitor = pipe_from_monitor;
        this.pipe_to_monitor = pipe_to_monitor;
    }

    public void run() {
        String taskSpec;
        String result;

        try {
            log("started");

            // Set up object IO with Scheduler
            this.obj_to_monitor = new ObjectOutputStream(pipe_to_monitor);
            this.obj_from_monitor = new ObjectInputStream(pipe_from_monitor);

            // Receive task specification from Scheduler
            taskSpec = ((Message) obj_from_monitor.readObject()).getBody();
            // Execute task
            result = execute(taskSpec);
            // Return result
            obj_to_monitor.writeObject(new Message(MessageType.TASK_RESULT, result));

            // Close IO channels
            pipe_from_monitor.close();
            pipe_to_monitor.close();

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
