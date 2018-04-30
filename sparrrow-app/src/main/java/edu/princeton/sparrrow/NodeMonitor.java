package edu.princeton.sparrrow;

import java.io.*;
import java.util.ArrayList;
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

    private ObjectOutputStream objToSched;

    // IO streams to and from Executor
    private PipedInputStream pipeFromExec;
    private PipedOutputStream pipeToExec;

    private ObjectOutputStream objToExec;

    public NodeMonitor(int id, ArrayList<PipedInputStream> pipesFromSched, ArrayList<PipedOutputStream> pipesToSched,
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
        try {
            log("started");

            // Set up object IO with Scheduler
            this.objToSched = new ObjectOutputStream(pipeToSched);
            // listen to scheduler
            SchedListener schedListener = new SchedListener(pipeFromSched);
            schedListener.start();

            // Set up object IO with Executor
            this.objToExec = new ObjectOutputStream(pipeToExec);

            ExecutorListener executorListener = new ExecutorListener(pipeFromExec, this);
            executorListener.start();

            log("finishing");
            while (true) {
                // This is here so the parent thread of SchedListener doesn't die
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // New thread that listens for messages from scheduler
    private class SchedListener extends Thread {

        private ObjectInputStream objFromSched;

        public SchedListener(PipedInputStream pipeFromSched) throws IOException {
            this.objFromSched = new ObjectInputStream(pipeFromSched);
        }

        public void run() {
            MessageContent m;
            ProbeContent probe;
            TaskSpecContent taskSpec;
            log("starting sched listener");
            while (true) {
                try {
                    m = ((Message) objFromSched.readObject()).getBody();
                    if (m instanceof ProbeContent) {
                        // Receive probe from scheduler
                        probe = (ProbeContent) m;
                        // Handle message
                        handleProbe(probe);
                    } else if (m instanceof TaskSpecContent){
                        // Receive task specification from Scheduler
                        taskSpec = (TaskSpecContent) m;
                        // Handle message
                        handleTaskSpec(taskSpec);
                    } else {
                        log("ERROR: received message with wrong type");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    break;

                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private void log(String text){
        System.out.println("Node Monitor: " + text);
    }

    private void sendProbeReply(ProbeContent pc) throws IOException {
//        Message m = new Message(MessageType.PROBE_REPLY, pc); // TODO: are these types right for the message?
//        objToSched.writeObject(m);
        log("sending probe reply");
        ProbeReplyContent probeReply = new ProbeReplyContent(pc.getJobID(), this.id);
        Message m = new Message(MessageType.PROBE_REPLY, probeReply);
        objToSched.writeObject(m);
    }

    private void handleProbe(ProbeContent pc) throws IOException{
        log("received probe from scheduler");

        // Add probe to queue
        queueProbe(pc);

        if (!executor_is_occupied && probeQueue.size() == 1) {
            // Send probe reply if ready to execute immediately
            sendProbeReply(pc);
        }
    }

    private void queueProbe(ProbeContent pc) throws IOException{
        log("adding probe to queue");

        probeQueue.add(pc);

        // TODO: can the queue be full?
    }

    private void handleTaskSpec(TaskSpecContent s) throws IOException{
        // Check that spec exists
        // TODO: what does null spec look like? task spec string will be null

        // Ensure executor is unoccupied
        if (executor_is_occupied) {
            log("ERROR: received task spec while executor is occupied");
        }

        // Remove the probe that was replied to
        // TODO: check that this spec matches?
        log("received task spec message from Scheduler, removing probe from queue");
        ProbeContent pc = probeQueue.poll();

        // Send spec to executor for execution
        log("sending task " + s.getSpec() + " to Executor");
        Message m = new Message(MessageType.TASK_SPEC, s);
        objToExec.writeObject(m);

        // Mark executor as occupied
        executor_is_occupied = true;
    }

    public synchronized void handleTaskResult(TaskResultContent s) throws IOException{
        // Mark executor as unoccupied
        executor_is_occupied = false;

        // Pass task result back to scheduler
        log("received result message from Executor, sending to Scheduler");
        Message m = new Message(MessageType.TASK_RESULT, s);
        objToSched.writeObject(m);

        // Request next task (associated with first probe in queue)
        ProbeContent pc = probeQueue.peek();
        if (pc != null) {
            // Send probe reply
            sendProbeReply(pc);
        }
    }

}
