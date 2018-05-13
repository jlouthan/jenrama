package edu.princeton.sparrrow;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class BatchScheduler extends Scheduler {
    private HashMap<UUID, PendingJob> pendingJobs;

    public BatchScheduler(int id, ServerSocket socketWithFe, ArrayList<Socket> socketsWithMonitors, int d) throws IOException {
        super(id, socketWithFe, socketsWithMonitors, d);

        // name the log file (overrides the super's name)
        this.logFile = new File("logs/batch_" + this.formattedDate + "_scheduler_" + this.id + ".log");

        pendingJobs = new HashMap<>();
    }

    // Upon receiving a job, send out d probes for each task
    @Override
    public synchronized void receivedJob(JobSpecContent m) throws IOException {
        log(id + " received job spec from Frontend, starting task sampling (jobId = " + m.getJobID() + ")");

        ArrayList<String> taskSpecs = new ArrayList<>(m.getTasks());
        UUID jobId = m.getJobID();

        Job job = new Job(m.getFrontendID(), m.getTasks());
        jobs.put(m.getJobID(), job);
        log(id + " received job spec from Frontend");

        // initialize stats
        job.probeStats = new Stats(jobId.toString());
        job.specStats = new Stats(jobId.toString());

        ArrayList<Integer> myMonitors;

        int j;
        int numTasks = taskSpecs.size();

        // Randomize node monitor ids to choose which to place tasks on
        Collections.shuffle(monitorIds);

        // Select d*m monitors to sample for this job
        myMonitors = new ArrayList<>();
        for(j = 0; j < d * numTasks; j++){
            myMonitors.add(monitorIds.get(j % numMonitors));
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

            // increment stats for probes
            job.probeStats.incrementCount(monitorId);
        }
    }

    @Override
    public synchronized void receivedSpecRequest(ProbeReplyContent m) throws IOException {
        TaskSpecContent taskSpec;
        DetailedProbeReplyContent mes = (DetailedProbeReplyContent) m;

        UUID jobId = m.getJobID();
        // If receive spec request for job that doesn't exist, show error
        if (!pendingJobs.containsKey(jobId)) {
            log("ERROR:  Received spec request for job " + jobId + " that doesn't exist.");
        }

        // Retrieve information for the task referred to in this probe
        PendingJob myJob = pendingJobs.get(jobId);
        ArrayList<String> taskSpecStrs = myJob.specs;
        int numTasks = taskSpecStrs.size();
        int dm = d * numTasks;

        // Identify the node monitor making the request
        int monitorId = mes.getMonitorID();

        // Store the value that the node monitor gave for its queue for this task
        myJob.replies.add(new MonitorElement(mes.getqLength(), monitorId));
        // TODO check that probe replies come from expected node monitors (the ones in MyMonitors)

        if(myJob.numReplies >= dm){
            super.log("ERROR: received too many probe replies for task " + jobId);
        }
        myJob.numReplies++;

        // If all our probes have returned, give the tasks to the best monitors
        if(myJob.numReplies == dm){
            log("finished sampling for job" + jobId + ", sending to NodeMonitors");

            // Sort monitor replies by queue length
            Collections.sort(myJob.replies);

            UUID taskId;
            int destId;
            for(int i = 0; i < numTasks; i++){
                // Create task spec to pass to node monitor
                taskId = UUID.randomUUID();
                taskSpec = new TaskSpecContent(jobId, taskId, this.id, taskSpecStrs.get(i));

                // Send the task to my favorite monitor
                Message spec = new Message(MessageType.TASK_SPEC, taskSpec);
                destId = myJob.replies.get(i).id;
                objToNodeMonitors.get(destId).writeObject(spec);

                log("Sending spec for task " + taskId + "to NodeMonitor " + destId);

                // increment stats for specs
                Job job = jobs.get(jobId);
                job.specStats.incrementCount(destId);

            }

        }
    }



    private class PendingJob {
        final UUID id;
        final LinkedList<MonitorElement> replies;
        final ArrayList<Integer> myMonitors;
        int numReplies = 0;
        final ArrayList<String> specs;

        PendingJob(UUID id, ArrayList<String> specs, ArrayList<Integer> myMonitors){
            this.id = id;
            this.replies = new LinkedList<>();
            this.myMonitors = myMonitors;
            this.specs = specs;
        }
    }
    private class MonitorElement implements Comparable<MonitorElement> {
        final int qLength;
        final int id;

        MonitorElement(int qLength, int id){
            this.qLength = qLength;
            this.id = id;
        }

        public int compareTo(MonitorElement o){
            return qLength - o.qLength;
        }
    }
}
