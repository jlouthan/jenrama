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

    private ObjectInputStream objFromFe;
    private ObjectOutputStream objToFe;

    // IO streams to and from NodeMonitor
    private PipedInputStream pipeFromNodeMonitor;
    private PipedOutputStream pipeToNodeMonitor;

    private ObjectOutputStream objToMonitor;

    private ConcurrentHashMap<UUID, Job> jobs;

    public Scheduler(int id, PipedInputStream pipeFromFe, PipedOutputStream pipeToFe,
                     PipedInputStream pipeFromNodeMonitor, PipedOutputStream pipeToNodeMonitor){
        this.id = id;

        this.pipeFromFe = pipeFromFe;
        this.pipeToFe = pipeToFe;

        this.pipeFromNodeMonitor = pipeFromNodeMonitor;
        this.pipeToNodeMonitor = pipeToNodeMonitor;

        jobs = new ConcurrentHashMap<>();

    }

    public void run() {
        JobSpecContent newJob;

        try {

            // Set up object IO with Frontend
            this.objToFe = new ObjectOutputStream(pipeToFe);
            this.objFromFe = new ObjectInputStream(pipeFromFe);


            // Set up object IO with NodeMonitor
            this.objToMonitor = new ObjectOutputStream(pipeToNodeMonitor);
            // listen to monitor
            MonitorListener monitorListener = new MonitorListener(pipeFromNodeMonitor, this);
            monitorListener.start();

            log("started");

            // Receive job from Frontend
            newJob = (JobSpecContent)((Message) objFromFe.readObject()).getBody();
            // Handle message
            receivedJob(newJob);

            // Close IO channels
            pipeFromFe.close();

            log("finishing");
            while (true) {
                // This is here so the parent thread of MonitorListener doesn't die
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
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
            this.taskResults = new ArrayList<String>();
            //numTasks = tasksRemaining.size();
            // TODO use above line instead once executor can process multiple tasks per job
            numTasks = 1;
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

        // Send reservations (probes) to node monitor. Currently sending all tasks to one monitor.
        for (int i = 0; i < j.tasksRemaining.size(); i++) {
            log("received job spec from Frontend, sending probe to NodeMonitor");
            ProbeContent probe = new ProbeContent(m.getJobID(), this.id);
            Message probeMessage = new Message(MessageType.PROBE, probe);
            objToMonitor.writeObject(probeMessage);
        }

        //TODO: Enable tracking multiple node monitor IDs and probing a subset of them;
    }

    public synchronized void receivedSpecRequest(ProbeReplyContent m) throws IOException {
        // Find the job containing the requested task spec
        UUID jobId = m.getJobID();
        String task = jobs.get(jobId).getNextTaskRemaining();

        // If there are no more tasks for the job, ignore the request from the Node Monitor
        if (task == null) {
            return;
        }
        log("received task spec request from NodeMonitor, sending task " + task + " to NodeMonitor");
        // Create one task spec to pass to node monitor
        TaskSpecContent taskSpec = new TaskSpecContent(jobId, UUID.randomUUID(), this.id, task);
        Message spec = new Message(MessageType.TASK_SPEC, taskSpec);
        objToMonitor.writeObject(spec);
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
