package edu.princeton.sparrrow;

import javax.management.monitor.Monitor;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.*;

public class BatchScheduler extends Scheduler {
    private HashMap<UUID, PendingJob> pendingJobs;

    public BatchScheduler(int id, PipedInputStream pipeFromFe, PipedOutputStream pipeToFe,
                          ArrayList<PipedInputStream> pipesFromNodeMonitor, ArrayList<PipedOutputStream> pipesToNodeMonitor, int d) {
        super(id, pipeFromFe, pipeToFe, pipesFromNodeMonitor, pipesToNodeMonitor, d);

        pendingJobs = new HashMap<>();
    }

    // Upon recieving a job, send out d probes for each task
    @Override
    public synchronized void receivedJob(JobSpecContent m) throws IOException {
        log(id + " received job spec from Frontend, starting task sampling (jobId = " + m.getJobID() + ")");

        ArrayList<String> taskSpecs = (ArrayList<String>) m.getTasks();
        UUID jobId = m.getJobID();

        Job job = new Job(m.getFrontendID(), m.getTasks());
        jobs.put(m.getJobID(), job);
        log(id + " received job spec from Frontend");

        ArrayList<Integer> myMonitors;

        int j;
        int numTasks = taskSpecs.size();

        // Randomize node monitor ids to choose which to place tasks on
        Collections.shuffle(monitorIds);

        // Select d monitors to sample for this task
        myMonitors = new ArrayList<>();
        for(j = 0; j < d * numTasks; j++){
            myMonitors.add(monitorIds.get(j % monitorIds.size()));
        }

        // Create probe message
        ProbeContent probe = new ProbeContent(m.getJobID(), this.id);
        Message probeMessage = new Message(MessageType.PROBE, probe);

        // Store task UUID -> PendingTask in hashmap so that we can find the job later
        PendingJob pj = new PendingJob(jobId, taskSpecs, myMonitors);
        pendingJobs.put(jobId, pj);

        for(int monitorId : myMonitors){
            log("sending job probe to monitor " + monitorId + "(jobId = " + jobId + ")");
            objToNodeMonitors.get(monitorId).writeObject(probeMessage);
        }
    }

    @Override
    public synchronized void receivedSpecRequest(ProbeReplyContent m) throws IOException {
        TaskSpecContent taskSpec;
        DetailedProbeReplyContent mes = (DetailedProbeReplyContent) m;

        // Retrieve information for the task referred to in this probe
        PendingJob myJob = pendingJobs.get(m.getJobID());
        UUID jobId = myJob.id;
        ArrayList<String> taskSpecStrs = myJob.specs;
        int numTasks = taskSpecStrs.size();
        int dm = myJob.myMonitors.size();

        // Identify the node monitor making the request
        int monitorId = m.getMonitorID();

        // Store the value that the node monitor gave for its queue for this task
        myJob.replies[myJob.myMonitors.indexOf(monitorId)] = mes.getqLength();
        if(myJob.numReplies >= d){
            super.log("ERROR: recieved too many probe replies for task " + jobId);
        }
        myJob.numReplies++;

        // If all our probes have returned, give the tasks to the best monitors
        if(myJob.numReplies == d){
            log("finished sampling for job" + jobId + ", sending to NodeMonitors");

            // Make a list of monitor elements
            int i;
            ArrayList<MonitorElement> monitors = new ArrayList<>();
            for(i = 0; i < dm; i++){
                monitors.add(new MonitorElement(myJob.replies[i], myJob.myMonitors.get(i)));
            }

            // Sort them by queue length
            Collections.sort(monitors);

            UUID taskId;
            int destId;
            for(i = 0; i < numTasks; i++){
                // Create task spec to pass to node monitor
                taskId = UUID.randomUUID();
                taskSpec = new TaskSpecContent(jobId, taskId, this.id, taskSpecStrs.get(i));

                // Send the task to my favorite monitor
                Message spec = new Message(MessageType.TASK_SPEC, taskSpec);
                destId = monitors.get(i).id;
                objToNodeMonitors.get(destId).writeObject(spec);

                log("Sending spec for task " + taskId + "to NodeMonitor " + destId);

            }

        }
    }



    private class PendingJob {
        final UUID id;
        final int[] replies;
        final ArrayList<Integer> myMonitors;
        int numReplies = 0;
        final ArrayList<String> specs;

        PendingJob(UUID id, ArrayList<String> specs, ArrayList<Integer> myMonitors){
            this.id = id;
            this.replies = new int[myMonitors.size()];
            this.myMonitors = myMonitors;
            this.specs = specs;
        }
    }
    private class MonitorElement implements Comparable<MonitorElement> {
        final int q;
        final int id;

        MonitorElement(int q, int id){
            this.q = q;
            this.id = id;
        }

        public int compareTo(MonitorElement o){
            return q - o.q;
        }
    }
}
