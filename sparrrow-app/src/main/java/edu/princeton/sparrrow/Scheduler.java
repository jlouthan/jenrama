package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * The scheduler receives jobs from frontend instances and coordinates
 * between node monitors, placing probes and scheduling job tasks
 * according to the scheduling policy.
 */

public class Scheduler implements Runnable {
    protected final int id;
    protected final int d;

    // IO streams to and from Frontend
    private Socket socketWithFe;
    private ObjectOutputStream objToFe;

    // IO streams to and from ann node monitors
    private ArrayList<Socket> socketsWithMonitors;

    private ArrayList<MonitorListener> monitorListeners;
    protected ArrayList<ObjectOutputStream> objToNodeMonitors;

    // data structure for storing state of jobs in flight
    protected ConcurrentHashMap<UUID, Job> jobs;

    // list of node monitor ids that will be shuffled to determine where to place task probes
    protected List<Integer> monitorIds;

    protected int numMonitors;

    public Scheduler(int id, ServerSocket socketWithFe, ArrayList<Socket> socketsWithMonitors, int d) throws IOException {
        this.id = id;
        this.d = d;

        this.socketWithFe = socketWithFe.accept();

        this.socketsWithMonitors = socketsWithMonitors;

        monitorListeners = new ArrayList<>();
        objToNodeMonitors = new ArrayList<>();

        jobs = new ConcurrentHashMap<>();

        numMonitors = socketsWithMonitors.size();
        // set up the list of node monitor ids
        monitorIds = new ArrayList<>();
        for(int i = 0; i < numMonitors; i++) {
            monitorIds.add(i);
        }


    }

    public void run() {

        try {

            // Set up object IO with Frontend
            this.objToFe = new ObjectOutputStream(socketWithFe.getOutputStream());
            FrontendListener frontendListener = new FrontendListener(socketWithFe.getInputStream(), this);
            frontendListener.start();

            // Set up object IO with NodeMonitors
            for (int i = 0; i < numMonitors; i++) {
                // Create a listener for each Node Monitor
                log("adding monitor listener " + i);
                MonitorListener monitorListener = new MonitorListener(socketsWithMonitors.get(i), this);
                monitorListeners.add(monitorListener);

                // Create a obj stream to each Node Monitor
                ObjectOutputStream objToMonitor = new ObjectOutputStream(socketsWithMonitors.get(i).getOutputStream());
                objToNodeMonitors.add(objToMonitor);
            }

            // start the listener to each node monitor
            for (int i = 0; i < numMonitors; i++) {
                monitorListeners.get(i).start();
            }

            log("started");


            while (true) {
                // This is here so the parent thread of MonitorListener doesn't die
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void log(String text){
        System.out.println("Scheduler[" + this.id + "]: " + text);
    }

    protected class Job {
        private int frontendId;
        private LinkedList<String> tasksRemaining;
        private ArrayList<String> taskResults;
        protected int numTasks;

        // Create a new job with no task results yet
        public Job(int frontendId, Collection<String> tasksRemaining) {
            this.frontendId = frontendId;
            this.tasksRemaining = (LinkedList) tasksRemaining;
            this.taskResults = new ArrayList<>();
            numTasks = tasksRemaining.size();
        }

        public String getNextTaskRemaining() {
            if (tasksRemaining.isEmpty()) {
                return null;
            }
            return tasksRemaining.removeFirst();
        }

        public boolean isComplete() {
            // currently there is no validation that the task results are for unique tasks
            log("Checking if job is complete; It has finished " + taskResults.size() + " tasks out of " + numTasks);
            return taskResults.size() == numTasks;
        }
    }

    public synchronized void receivedJob(JobSpecContent m) throws IOException{
        // Store some state about the job, including remaining tasks to schedule
        Job j = new Job(m.getFrontendID(), m.getTasks());
        jobs.put(m.getJobID(), j);

        log(id + " received job spec from Frontend");
        // Randomize node monitor ids to choose which to place tasks on
        Collections.shuffle(monitorIds);

        log(this.id + " d * m is: " + d * j.numTasks);
        // Send reservations (probes) to d*m selected node monitors
        for (int i = 0; i < d * j.numTasks; i++) {
            int monitorId = monitorIds.get(i % numMonitors);

            ProbeContent probe = new ProbeContent(m.getJobID(), this.id);
            Message probeMessage = new Message(MessageType.PROBE, probe);

            log("sending probe to monitor " + monitorId);
            objToNodeMonitors.get(monitorId).writeObject(probeMessage);
        }

    }

    public synchronized void receivedSpecRequest(ProbeReplyContent m) throws IOException {
        TaskSpecContent taskSpec;

        // Find the job containing the requested task spec
        UUID jobId = m.getJobID();
        // If receive spec request for job that doesn't exist, show error
        if (!jobs.containsKey(jobId)) {
            log("ERROR:  Received spec request for job " + jobId + " that doesn't exist.");
        }
        String task = jobs.get(jobId).getNextTaskRemaining();
        // Identify the node monitor making the request
        int monitorId = m.getMonitorID();

        // If there are no more tasks for the job, send null spec back to Node Monitor
        if (task == null) {
            log("received task spec request for finished job, sending null reply");
            taskSpec = new TaskSpecContent(jobId, null, this.id, null);
        } else {
            log("received task spec request from NodeMonitor, sending task " + task + " to NodeMonitor");
            // Create one task spec to pass to node monitor
            taskSpec = new TaskSpecContent(jobId, UUID.randomUUID(), this.id, task);
        }
        Message spec = new Message(MessageType.TASK_SPEC, taskSpec);
        objToNodeMonitors.get(monitorId).writeObject(spec);
    }

    public synchronized void receivedResult(TaskResultContent m) throws IOException{
        // collect task result
        UUID jobId = m.getJobID();
        Job job = jobs.get(jobId);
        job.taskResults.add(m.getResult());

        // if task is finished, return result to frontend
        if (job.isComplete()) {
            log("received task result from NodeMonitor, sending job result to Frontend");
            JobResultContent newMessage = new JobResultContent(jobId, job.taskResults);

            Message reply = new Message(MessageType.JOB_RESULT, newMessage);
            objToFe.writeObject(reply);
        }
    }

}
