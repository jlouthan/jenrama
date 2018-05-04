package edu.princeton.sparrrow;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * The scheduler receives jobs from frontend instances and coordinates
 * between node monitors, placing probes and scheduling job tasks
 * according to the scheduling policy.
 */

public class Scheduler implements Runnable {
    private final int id;

    // IO streams to and from Frontend
    private PipedInputStream pipeFromFe;
    private PipedOutputStream pipeToFe;
    private ObjectOutputStream objToFe;

    // IO streams to and from ann node monitors
    private ArrayList<PipedInputStream> pipesFromNodeMonitor;
    private ArrayList<PipedOutputStream> pipesToNodeMonitor;

    private ArrayList<MonitorListener> monitorListeners;
    private ArrayList<ObjectOutputStream> objToNodeMonitors;

    // data structure for storing state of jobs in flight
    private ConcurrentHashMap<UUID, Job> jobs;

    public Scheduler(int id, PipedInputStream pipeFromFe, PipedOutputStream pipeToFe,
                     ArrayList<PipedInputStream> pipesFromNodeMonitor, ArrayList<PipedOutputStream> pipesToNodeMonitor) throws IOException {
        this.id = id;

        this.pipeFromFe = pipeFromFe;
        this.pipeToFe = pipeToFe;

        this.pipesFromNodeMonitor = pipesFromNodeMonitor;
        this.pipesToNodeMonitor = pipesToNodeMonitor;

        monitorListeners = new ArrayList<>();
        objToNodeMonitors = new ArrayList<>();

        jobs = new ConcurrentHashMap<>();

    }

    public void run() {

        try {

            // Set up object IO with Frontend
            this.objToFe = new ObjectOutputStream(pipeToFe);
            FrontendListener frontendListener = new FrontendListener(pipeFromFe, this);
            frontendListener.start();

            // Set up object IO with NodeMonitor
            //TODO this shouldn't need to be in here? or at least not hard-wired like this
            int numExecutors = 5;
            int startIndex = numExecutors * this.id;

            for (int i = startIndex; i < startIndex + numExecutors; i++) {
                // Create a listener for each Node Monitor
                log("Adding monitor listener " + i + " in scheduler " + this.id);
                MonitorListener monitorListener = new MonitorListener(pipesFromNodeMonitor.get(i), this);
                monitorListeners.add(monitorListener);

                // Create a obj stream to each Node Monitor
                ObjectOutputStream objToMonitor = new ObjectOutputStream(pipesToNodeMonitor.get(i));
                objToNodeMonitors.add(objToMonitor);
            }

            // start the listener to each node monitor
            for (int i = 0; i < numExecutors; i++) {
                monitorListeners.get(i).start();
            }

            log("started id " + id);


            while (true) {
                // This is here so the parent thread of MonitorListener doesn't die
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void log(String text){
        System.out.println("Scheduler: " + text);
    }

    private class Job {
        private int frontendId;
        private LinkedList<String> tasksRemaining;
        private ArrayList<String> taskResults;
        private int numTasks;

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
            return taskResults.size() == numTasks;
        }
    }

    public synchronized void receivedJob(JobSpecContent m) throws IOException{
        // Store some state about the job, including remaining tasks to schedule
        Job j = new Job(m.getFrontendID(), m.getTasks());
        jobs.put(m.getJobID(), j);

        log(id + " received job spec from Frontend, sending probes to NodeMonitor");
        // Send reservations (probes) to node monitor. Currently sending all tasks to one monitor.
        for (int i = 0; i < j.numTasks; i++) {
            ProbeContent probe = new ProbeContent(m.getJobID(), this.id);
            Message probeMessage = new Message(MessageType.PROBE, probe);
            //objToMonitor.writeObject(probeMessage);
            // TODO temporarily only write to first node monitor
            objToNodeMonitors.get(0).writeObject(probeMessage);
        }

        //TODO: Enable tracking multiple node monitor IDs and probing a subset of them;
    }

    public synchronized void receivedSpecRequest(ProbeReplyContent m) throws IOException {
        // Find the job containing the requested task spec
        UUID jobId = m.getJobID();
        String task = jobs.get(jobId).getNextTaskRemaining();
        // Identify the node monitor making the request
        int monitorId = m.getMonitorID();

        // If there are no more tasks for the job, ignore the request from the Node Monitor
        if (task == null) {
            return;
        }
        log("received task spec request from NodeMonitor, sending task " + task + " to NodeMonitor");
        // Create one task spec to pass to node monitor
        TaskSpecContent taskSpec = new TaskSpecContent(jobId, UUID.randomUUID(), this.id, task);
        Message spec = new Message(MessageType.TASK_SPEC, taskSpec);
        objToNodeMonitors.get(monitorId).writeObject(spec);
//        objToMonitor.writeObject(spec);
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
