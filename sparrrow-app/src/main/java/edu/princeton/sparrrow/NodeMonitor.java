package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

/**
 * The node monitor receives probes from schedulers, communicates with schedulers
 * to coordinate receiving job tasks, and sends those tasks to its executor.
 */

public class NodeMonitor implements Runnable {
    protected final int id;
    private CountDownLatch done;
    protected boolean executor_is_occupied;
    private Queue<ProbeContent> probeQueue;

    // IO streams to and from Schedulers
    private ArrayList<Socket> socketsWithScheds;
    private ArrayList<ServerSocket> socketsWithSchedsWaiting;
    protected ArrayList<ObjectOutputStream> objsToScheds;
    private ArrayList<SchedListener> schedListeners;
    private HashSet<Integer> deadSchedulers;

    // IO streams to and from Executor
    private Socket socketWithExec;
    protected ObjectOutputStream objToExec;

    public NodeMonitor(int id, ArrayList<ServerSocket> socketsWithSchedsWaiting, Socket socketWithExec) throws IOException {
        this.id = id;
        this.executor_is_occupied = false;
        this.probeQueue = new LinkedList<>();
        this.socketsWithSchedsWaiting = socketsWithSchedsWaiting;
        this.socketsWithScheds = new ArrayList<>();

        this.deadSchedulers = new HashSet<>();
        this.done = new CountDownLatch(socketsWithSchedsWaiting.size());

        this.objsToScheds = new ArrayList<>();

        this.schedListeners = new ArrayList<>();

        this.socketWithExec = socketWithExec;
    }

    public void run() {
        try {
            log("started");

            // This will block until all the expected schedulers connect to their respective sockets
            for(ServerSocket ss: socketsWithSchedsWaiting){
                log("trying to accept");
                this.socketsWithScheds.add(ss.accept());
            }

            // Set up object IO and listener with Schedulers
            int numSchedulers = this.socketsWithScheds.size();
            log("adding obj output streams and sched listeners");
            for (int i = 0; i < numSchedulers; i++) {
                ObjectOutputStream objToSched = new ObjectOutputStream(socketsWithScheds.get(i).getOutputStream());
                this.objsToScheds.add(objToSched);

                SchedListener schedListener = new SchedListener(socketsWithScheds.get(i).getInputStream(), this);
                this.schedListeners.add(schedListener);
            }

            // Start listening to Schedulers
            log("starting sched listeners");
            for (int i = 0; i < numSchedulers; i++) {
                schedListeners.get(i).start();
            }

            // Set up object IO with Executor
            this.objToExec = new ObjectOutputStream(socketWithExec.getOutputStream());

            // Start listening to Executor
            ExecutorListener executorListener = new ExecutorListener(socketWithExec.getInputStream(), this);
            log("starting executor listener");
            executorListener.start();

            log("finishing");


            done.await();
            Thread.sleep(1000);

            // Sleep to make sure that Executor gets my done message
            //Thread.sleep(1000);

            log("Got here A");

            for(SchedListener listener : schedListeners){
                listener.done = true;
            }
            log("Got here B");

            for(SchedListener listener : schedListeners){
                listener.join();
            }

            socketWithExec.close();
            log("Got here C");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void log(String text){
        System.out.println("Node Monitor[" + this.id + "]: " + text);
    }

    private void sendProbeReply(ProbeContent pc) throws IOException {
        // Determine correct scheduler to write to
        int destination_scheduler = pc.getSchedID();
        ObjectOutputStream objToSched = this.objsToScheds.get(destination_scheduler);

        log("sending probe reply for job " + pc.getJobID() + " to scheduler " + destination_scheduler);

        // Send probe reply to (request task spec from) scheduler
        ProbeReplyContent probeReply = new ProbeReplyContent(pc.getJobID(), this.id);
        Message m = new Message(MessageType.PROBE_REPLY, probeReply);
        objToSched.writeObject(m);
    }

    // Gets probe from head of queue and if it exists, sends reply to
    // (requests task spec from) scheduler
    private void sendNextProbeReply() throws IOException {
        ProbeContent pc = probeQueue.peek();

        while(pc != null && deadSchedulers.contains(pc.getSchedID())){
            probeQueue.poll();
            pc = probeQueue.peek();
        }
        if (pc != null) {
            sendProbeReply(pc);
        } else {
            log("no more probes to send");
        }
    }

    public synchronized void handleProbe(ProbeContent pc) throws IOException{
        log("received probe from scheduler " + pc.getSchedID());

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
            log("ERROR: received task spec but was not expecting one (probe queue is empty)");
            return;
        }
        if (!s.getJobID().equals(pc.getJobID()) || s.getSchedID() != pc.getSchedID()) {
            log("ERROR: received task spec for job " + s.getJobID()
                    + " from scheduler " + s.getSchedID()
                    + " that does not match requested spec for job " + pc.getJobID()
                    + " from scheduler " + pc.getSchedID());
            return;
        }
        // Remove the probe that was replied to
        log("received task spec message from scheduler, removing probe from queue");
        probeQueue.poll();

        // If spec does not exist (if its job has finished), ask for a new spec
        if (s.getSpec() == null) {
            log("task spec received was null, sending next probe reply");
            sendNextProbeReply();
            return;
        }

        // Ensure executor is unoccupied
        if (executor_is_occupied) {
            log("ERROR: received task spec while executor is occupied");
            // TODO: make execution queue
            return;
        }

        // Send spec to executor for execution
        log("sending task " + s.getSpec() + " to executor");
        Message m = new Message(MessageType.TASK_SPEC, s);
        objToExec.writeObject(m);

        // Mark executor as occupied
        executor_is_occupied = true;
    }

    public synchronized void handleTaskResult(TaskResultContent s) throws IOException{
        // Mark executor as unoccupied
        executor_is_occupied = false;

        // Determine correct scheduler to write to
        int destination_scheduler = s.getSchedID();
        ObjectOutputStream objToSched = this.objsToScheds.get(destination_scheduler);

        // Pass task result back to scheduler
        log("received result message from executor, sending to scheduler " + destination_scheduler);
        Message m = new Message(MessageType.TASK_RESULT, s);
        objToSched.writeObject(m);

        // Request next task (associated with first probe in queue)
        sendNextProbeReply();
    }

    public synchronized void handleDoneMessage(DoneContent m) throws IOException {
        int schedId = m.getId();
        if (deadSchedulers.contains(schedId)){
            log("ERROR: recieved multiple Done messages from Scheduler " + schedId);
        } else {
            deadSchedulers.add(schedId);
            socketsWithScheds.get(schedId).close();
            done.countDown();

            log("recieved done message from scheduler [" + schedId + "], waiting for " + done.getCount() + " more");
            if (done.getCount() == 0){
                log("received done messages from all schedulers, shutting down");
                DoneContent myDone = new DoneContent(id);
                Message doneM = new Message(MessageType.DONE, myDone);
                objToExec.writeObject(doneM);
            }
        }
    }
}
