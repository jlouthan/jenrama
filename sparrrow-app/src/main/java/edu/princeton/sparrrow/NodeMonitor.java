package edu.princeton.sparrrow;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;

/**
 * The node monitor receives probes from schedulers, communicates with schedulers
 * to coordinate receiving job tasks, and sends those tasks to its executor.
 */

public class NodeMonitor implements Runnable {
    private final int id;
    private boolean executor_is_occupied;
    private Queue<ProbeContent> probeQueue;

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
        this.executor_is_occupied = false;
        this.probeQueue = new LinkedList<>();

        this.pipeFromSched = pipeFromSched;
        this.pipeToSched = pipeToSched;

        this.pipeFromExec = pipeFromExec;
        this.pipeToExec = pipeToExec;
    }

    public void run() {
        TaskResultContent taskResult;

        try {
            log("started");

            // Set up object IO with Scheduler
            this.objToSched = new ObjectOutputStream(pipeToSched);
            this.objFromSched = new ObjectInputStream(pipeFromSched);

            // Set up object IO with Executor
            this.objToExec = new ObjectOutputStream(pipeToExec);
            this.objFromExec = new ObjectInputStream(pipeFromExec);

            // Receive and handle message from Scheduler
            Message m = (Message) objFromSched.readObject();
            MessageType type = m.getType();
            if (type == MessageType.PROBE) {
                // Process a probe
                handleProbe((ProbeContent) m.getBody());
            } else if (type == MessageType.TASK_SPEC) {
                // Handle a task specification
                handleTaskSpec((TaskSpecContent) m.getBody());
            } else {
                // TODO: error case
                ;
            }

            // Receive task result from Executor
            taskResult = (TaskResultContent)((Message) objFromExec.readObject()).getBody();
            // Handle task result
            handleTaskResult(taskResult);

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

    private void sendProbeReply(ProbeContent pc) throws IOException {
        Message m = new Message(MessageType.PROBE_REPLY, pc); // TODO: are these types right for the message?
        objToSched.writeObject(m);
    }

    private void handleProbe(ProbeContent pc) throws IOException{
        // Check status of executor
        if (executor_is_occupied) {
            // Add probe to queue
            queueProbe(pc);
        } else {
            // Send probe reply
            sendProbeReply(pc);
        }
    }

    private void queueProbe(ProbeContent pc) throws IOException{
        probeQueue.add(pc);

        // TODO: can the queue be full?
    }

    private void handleTaskSpec(TaskSpecContent s) throws IOException{
        // Check that spec exists
        // TODO: what does null spec look like? task spec string will be null

        // Ensure executor is unoccupied
        if (executor_is_occupied) {
            // TODO: error? wait?
            ;
        }

        // Send spec to executor for execution
        log("received task spec message from Scheduler, sending to Executor");
        Message m = new Message(MessageType.TASK_SPEC, s);
        objToExec.writeObject(m);

        // Mark executor as occupied
        executor_is_occupied = true;
    }

    private void handleTaskResult(TaskResultContent s) throws IOException{
        // Mark executor as unoccupied
        executor_is_occupied = false;

        // Pass task result back to scheduler
        log("received result message from Executor, sending to Scheduler");
        Message m = new Message(MessageType.TASK_RESULT, s);
        objToSched.writeObject(m);

        // Request next task (associated with first probe in queue)
        ProbeContent pc = probeQueue.poll();
        if (pc != null) {
            // Send probe reply
            sendProbeReply(pc);
        }
    }

}
