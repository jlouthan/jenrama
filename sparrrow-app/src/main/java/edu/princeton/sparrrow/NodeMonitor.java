package edu.princeton.sparrrow;

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
                        receivedReservation(probe);
                    } else {
                        // Receive task specification from Scheduler
                        taskSpec = (TaskSpecContent) m;
                        // Handle message
                        receivedSpec(taskSpec);
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
        log("received task spec message from Scheduler, sending task " + s.getSpec() + " to Executor");
        Message m = new Message(MessageType.TASK_SPEC, s);
        objToExec.writeObject(m);
    }

    public void receivedResult(TaskResultContent s) throws IOException{
        // Pass task result back to scheduler
        log("received result message from Executor, sending to Scheduler");
        Message m = new Message(MessageType.TASK_RESULT, s);
        objToSched.writeObject(m);
    }

}
