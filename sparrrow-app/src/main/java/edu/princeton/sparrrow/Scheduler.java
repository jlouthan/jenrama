package edu.princeton.sparrrow;

import org.json.JSONObject;

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

    private ObjectInputStream objFromMonitor;
    private ObjectOutputStream objToMonitor;

    public Scheduler(int id, PipedInputStream pipeFromFe, PipedOutputStream pipeToFe,
                     PipedInputStream pipeFromNodeMonitor, PipedOutputStream pipeToNodeMonitor){
        this.id = id;

        this.pipeFromFe = pipeFromFe;
        this.pipeToFe = pipeToFe;

        this.pipeFromNodeMonitor = pipeFromNodeMonitor;
        this.pipeToNodeMonitor = pipeToNodeMonitor;
    }

    public void run() {
        JobSpecContent newJob;
        TaskResultContent taskResult;
        ProbeReplyContent probeReply;

        try {

            // Set up object IO with Frontend
            this.objToFe = new ObjectOutputStream(pipeToFe);
            this.objFromFe = new ObjectInputStream(pipeFromFe);


            // Set up object IO with NodeMonitor
            this.objToMonitor = new ObjectOutputStream(pipeToNodeMonitor);
            this.objFromMonitor = new ObjectInputStream(pipeFromNodeMonitor);

            log("started");

            // Receive job from Frontend
            newJob = (JobSpecContent)((Message) objFromFe.readObject()).getBody();
            // Handle message
            receivedJob(newJob);

            // Receive request for task spec from NodeMonitor
            probeReply = (ProbeReplyContent) ((Message) objFromMonitor.readObject()).getBody();
            // Handle message
            receivedSpecRequest(probeReply);

            // Receive task result from NodeMonitor
            taskResult = (TaskResultContent)((Message) objFromMonitor.readObject()).getBody();
            // Handle message
            receivedResult(taskResult);

            // Close IO channels
            pipeFromFe.close();
            pipeToFe.close();
            pipeToNodeMonitor.close();
            pipeFromNodeMonitor.close();

            log("finishing");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void log(String text){
        System.out.println("Scheduler: " + text);
    }

    //TODO move to its own class?
    private class Job {
        private int frontendId;
        private LinkedList<String> tasksRemaining;
        //TODO add tasksInFlight or something?? to track sent tasks that aren't yet completed?
        private ArrayList<String> taskResults;

        // Create a new job with no task results yet
        public Job(int frontendId, Collection<String> tasksRemaining) {
            this.frontendId = frontendId;
            this.tasksRemaining = (LinkedList) tasksRemaining;
            this.taskResults = new ArrayList<String>();
        }

        public String getNextTaskRemaining() {
            if (tasksRemaining.isEmpty()) {
                return null;
            }
            return tasksRemaining.removeFirst();
        }
    }

    // TODO move to top of class?
    private ConcurrentHashMap<UUID, Job> jobs = new ConcurrentHashMap<>();

    private void receivedJob(JobSpecContent m) throws IOException{
        //TODO: Store some state about the job, including remaining tasks to schedule
        Job j = new Job(m.getFrontendID(), m.getTasks());
        jobs.put(m.getJobID(), j);

        //TODO: Send reservations (probes) to node monitor
        log("received job spec from Frontend, sending probe to NodeMonitor");
        ProbeContent probe = new ProbeContent(m.getJobID(), this.id);
        Message probeMessage = new Message(MessageType.PROBE, probe);
        objToMonitor.writeObject(probeMessage);

        //TODO: Enable tracking multiple node monitor IDs and probing a subset of them;
    }

    private void receivedSpecRequest(ProbeReplyContent m) throws IOException{
        log("received task spec request from NodeMonitor, sending task spec to NodeMonitor");

        // Find the job containing the requested task spec
        UUID jobId = m.getJobID();
        String task = jobs.get(jobId).getNextTaskRemaining();

        // If there are no more tasks for the job, ignore the request from the Node Monitor
        if (task == null) {
            return;
        }
        // Create one task spec to pass to node monitor
        TaskSpecContent taskSpec = new TaskSpecContent(jobId, UUID.randomUUID(), this.id, task);
        Message spec = new Message(MessageType.TASK_SPEC, taskSpec);
        objToMonitor.writeObject(spec);
    }

    private void receivedResult(TaskResultContent m) throws IOException{
        // TODO: collect task result, return if it's done

        // For now, just pass it back up to the Frontend
        log("received task result from NodeMonitor, sending job result to Frontend");
        ArrayList<String> results = new ArrayList<String>();
        results.add(m.getResult());
        JobResultContent newMessage = new JobResultContent(m.getJobID(), results);

        Message reply = new Message(MessageType.JOB_RESULT, newMessage);
        objToFe.writeObject(reply);
    }
}
