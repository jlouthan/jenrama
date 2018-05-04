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

    // IO streams to and from Schedulers
    private ArrayList<PipedInputStream> pipesFromScheds;
    private ArrayList<PipedOutputStream> pipesToScheds;
    private ArrayList<ObjectOutputStream> objsToScheds;
    private ArrayList<SchedListener> schedListeners;

    // IO streams to and from Executor
    private PipedInputStream pipeFromExec;
    private PipedOutputStream pipeToExec;
    private ObjectOutputStream objToExec;

    public NodeMonitor(int id, ArrayList<PipedInputStream> pipesFromScheds, ArrayList<PipedOutputStream> pipesToScheds,
                       PipedInputStream pipeFromExec, PipedOutputStream pipeToExec){

        this.id = id;
        this.executor_is_occupied = false;
        this.probeQueue = new LinkedList<>();

        this.pipesFromScheds = pipesFromScheds;
        this.pipesToScheds = pipesToScheds;
        this.objsToScheds = new ArrayList<>();
        this.schedListeners = new ArrayList<>();

        this.pipeFromExec = pipeFromExec;
        this.pipeToExec = pipeToExec;
    }

    public void run() {
        try {
            log("started");

            // Set up object IO with Schedulers
            int numSchedulers = this.pipesFromScheds.size();
            for (int i = 0; i < numSchedulers; i++) {
                SchedListener schedListener = new SchedListener(pipesFromScheds.get(i), this);
                this.schedListeners.add(schedListener);
                log("Added scheduler listener " + i + " in node monitor " + this.id);

                ObjectOutputStream objToSched = new ObjectOutputStream(pipesToScheds.get(i));
                this.objsToScheds.add(objToSched);
            }

            // listen to Schedulers
            log("starting sched listeners");
            for (int i = 0; i < numSchedulers; i++) {
                schedListeners.get(i).start();
            }

            // Set up object IO with Executor
            this.objToExec = new ObjectOutputStream(pipeToExec);

            ExecutorListener executorListener = new ExecutorListener(pipeFromExec, this);
            log("starting executor listener");
            executorListener.start();

            log("finishing");
            while (true) {
                // This is here so the parent thread of SchedListener doesn't die
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void log(String text){
        System.out.println("Node Monitor: " + text);
    }

    private void sendProbeReply(ProbeContent pc) throws IOException {
        ObjectOutputStream objToSched;

        // Determine correct scheduler to write to
        objToSched = this.objsToScheds.get(0); // TODO: this

        log("sending probe reply");
        ProbeReplyContent probeReply = new ProbeReplyContent(pc.getJobID(), this.id);
        Message m = new Message(MessageType.PROBE_REPLY, probeReply);
        objToSched.writeObject(m);
    }

    public synchronized void handleProbe(ProbeContent pc) throws IOException{
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
    }

    public synchronized void handleTaskSpec(TaskSpecContent s) throws IOException{
        // Check that the spec matches the first probe
        ProbeContent pc = probeQueue.peek();
        if (pc == null) {
            log("ERROR: received task spec but probe queue is empty");
            return;
        }
        // TODO: is there a better way to compare these? UUID?
        if (s.getJobID() != pc.getJobID() || s.getSchedID() != pc.getSchedID()) {
            log("ERROR: received task spec that does not match requested spec");
            return;
        }
        // Remove the probe that was replied to
        log("received task spec message from Scheduler, removing probe from queue");
        probeQueue.poll();


        // If spec does not exist, ask for a new spec by requesting
        // the next task (associated with first probe in queue)
        if (s.getSpec() == null) {
            pc = probeQueue.poll();
            if (pc != null) {
                // Send probe reply
                sendProbeReply(pc);
            }
        }

        // Ensure executor is unoccupied
        if (executor_is_occupied) {
            log("ERROR: received task spec while executor is occupied");
            // TODO: make execution queue
            return;
        }

        // Send spec to executor for execution
        log("sending task " + s.getSpec() + " to Executor");
        Message m = new Message(MessageType.TASK_SPEC, s);
        objToExec.writeObject(m);

        // Mark executor as occupied
        executor_is_occupied = true;
    }

    public synchronized void handleTaskResult(TaskResultContent s) throws IOException{
        ObjectOutputStream objToSched;

        // Mark executor as unoccupied
        executor_is_occupied = false;

        // Determine correct scheduler to write to
        objToSched = this.objsToScheds.get(0); // TODO: this
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
