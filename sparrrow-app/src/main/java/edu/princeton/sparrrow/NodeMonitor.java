package edu.princeton.sparrrow;

import com.sun.tracing.Probe;

import java.io.*;

/**
 * The node monitor receives probes from schedulers, communicates with schedulers
 * to coordinate receiving job tasks, and sends those tasks to its executor.
 */

public class NodeMonitor implements Runnable {
    private final int id;

    // IO streams to and from Scheduler
    private PipedInputStream pipeFromSched;
    private PipedOutputStream pipeToSched;

    private ObjectInputStream objFromSched;
    private ObjectOutputStream objToSched;

    // IO streams to and from Executor
    private PipedInputStream pipeFromExec;
    private PipedOutputStream pipeToExec;

    private ObjectInputStream objFromExec;
    private ObjectOutputStream objToExec;

    public NodeMonitor(int id, PipedInputStream pipeFromSched, PipedOutputStream pipeToSched,
                     PipedInputStream pipeFromExec, PipedOutputStream pipeToExec){
        this.id = id;

        this.pipeFromSched = pipeFromSched;
        this.pipeToSched = pipeToSched;

        this.pipeFromExec = pipeFromExec;
        this.pipeToExec = pipeToExec;
    }

    public void run() {
        TaskSpecContent taskSpec;
        TaskResultContent taskResult;
        ProbeContent probe;

        try {
            log("started");

            // Set up object IO with Scheduler
            this.objToSched = new ObjectOutputStream(pipeToSched);
            this.objFromSched = new ObjectInputStream(pipeFromSched);

            // Set up object IO with Executor
            this.objToExec = new ObjectOutputStream(pipeToExec);
            this.objFromExec = new ObjectInputStream(pipeFromExec);

            // Receive probe from Scheduler
            probe = (ProbeContent) ((Message) objFromSched.readObject()).getBody();
            // Handle message
            receivedReservation(probe);

            // Receive task specification from Scheduler
            taskSpec = (TaskSpecContent)((Message) objFromSched.readObject()).getBody();
            // Handle message
            receivedSpec(taskSpec);

            // Receive task result from Executor
            taskResult = (TaskResultContent)((Message) objFromExec.readObject()).getBody();
            // Handle message
            receivedResult(taskResult);

            // Close IO channels
            pipeFromExec.close();
            pipeToExec.close();
            pipeFromSched.close();
            pipeToSched.close();

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


    private void receivedReservation(ProbeContent pc) throws IOException{
        // TODO: add reservation to queue (with enough info to request spec from Scheduler later)

        // TODO: This is placeholder that immediately responds to scheduler w/o any queuing
        log("received probe from scheduler, replying with probe reply");
        ProbeReplyContent probeReply = new ProbeReplyContent(pc.getJobID(), this.id);
        Message m = new Message(MessageType.PROBE_REPLY, probeReply);
        objToSched.writeObject(m);
    }

    private void receivedSpec(TaskSpecContent s) throws IOException{
        // Send spec to executor for execution
        log("received task spec message from Scheduler, sending to Executor");
        Message m = new Message(MessageType.TASK_SPEC, s);
        objToExec.writeObject(m);
    }

    private void receivedResult(TaskResultContent s) throws IOException{
        // Pass task result back to scheduler
        log("received result message from Executor, sending to Scheduler");
        Message m = new Message(MessageType.TASK_RESULT, s);
        objToSched.writeObject(m);
    }

}
