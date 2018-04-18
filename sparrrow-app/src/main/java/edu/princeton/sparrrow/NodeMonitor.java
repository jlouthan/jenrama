package edu.princeton.sparrrow;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.*;

/**
 * The node monitor receives probes from schedulers, communicates with schedulers
 * to coordinate receiving job tasks, and sends those tasks to its executor.
 */

public class NodeMonitor implements Runnable {

    // IO streams to and from Scheduler
    private PipedInputStream pipe_from_sched;
    private PipedOutputStream pipe_to_sched;

    private ObjectInputStream obj_from_sched;
    private ObjectOutputStream obj_to_sched;

    // IO streams to and from Executor
    private PipedInputStream pipe_from_exec;
    private PipedOutputStream pipe_to_exec;

    private ObjectInputStream obj_from_exec;
    private ObjectOutputStream obj_to_exec;

    public NodeMonitor(PipedInputStream pipe_from_sched, PipedOutputStream pipe_to_sched,
                     PipedInputStream pipe_from_exec, PipedOutputStream pipe_to_exec){
        this.pipe_from_sched = pipe_from_sched;
        this.pipe_to_sched = pipe_to_sched;

        this.pipe_from_exec = pipe_from_exec;
        this.pipe_to_exec = pipe_to_exec;
    }

    public void run() {
        String taskSpec;
        String taskResult;

        try {
            log("started");

            // Set up object IO with Scheduler
            this.obj_to_sched = new ObjectOutputStream(pipe_to_sched);
            this.obj_from_sched = new ObjectInputStream(pipe_from_sched);

            // Set up object IO with Executor
            this.obj_to_exec = new ObjectOutputStream(pipe_to_exec);
            this.obj_from_exec = new ObjectInputStream(pipe_from_exec);

            // Receive task specification from Scheduler
            taskSpec = ((Message) obj_from_sched.readObject()).getBody();
            // Handle message
            receivedSpec(taskSpec);

            // Receive task result from Executor
            taskResult = ((Message) obj_from_exec.readObject()).getBody();
            // Handle message
            receivedResult(taskResult);

            // Close IO channels
            pipe_from_exec.close();
            pipe_to_exec.close();
            pipe_from_sched.close();
            pipe_to_sched.close();

            log("finishing");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    private void log(String text){
        System.out.println("Node Monitor: " + text);
    }


    private void receivedReservation(String m) throws IOException{
        // TODO: add reservation to queue (with enough info to request spec from Scheduler later)
    }

    private void receivedSpec(String s) throws IOException{
        // Send spec to executor for execution
        log("received task spec message from Scheduler, sending to Executor");
        Message m = new Message(MessageType.TASK_SPEC, s);
        obj_to_exec.writeObject(m);
    }

    private void receivedResult(String s) throws IOException{
        // Pass task result back to scheduler
        log("received result message from Executor, sending to Scheduler");
        Message m = new Message(MessageType.TASK_RESULT, s);
        obj_to_sched.writeObject(m);
    }

}
