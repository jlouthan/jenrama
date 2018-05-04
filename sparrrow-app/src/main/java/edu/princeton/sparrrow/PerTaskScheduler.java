package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.*;

public class PerTaskScheduler extends Scheduler {
        private HashSet<UUID> takenTasks;
        private HashMap<UUID, UUID> task2job;
        private HashMap<UUID, String> task2spec;


        public PerTaskScheduler(int id, PipedInputStream pipeFromFe, PipedOutputStream pipeToFe,
                               ArrayList<PipedInputStream> pipesFromNodeMonitor, ArrayList<PipedOutputStream> pipesToNodeMonitor, int d) {
            super(id, pipeFromFe, pipeToFe, pipesFromNodeMonitor, pipesToNodeMonitor, d);
            takenTasks = new HashSet<>();
            task2job = new HashMap<>();
            task2spec = new HashMap<>();
        }

        // Upon recieving a job, send out d probes for each task
        // PUTS THE TASK ID IN THE JOB ID FIELD
        @Override
        public synchronized void receivedJob(JobSpecContent m) throws IOException {
            log(id + " received job spec from Frontend, starting task sampling (jobId = " + m.getJobID() + ")");

            String[] taskSpecs = (String[]) m.getTasks().toArray();

            ArrayList<Integer> myMonitors;
            UUID taskId;
            for (int i = 0; i < taskSpecs.length; i++) {
                // Randomize node monitor ids to choose which to place tasks on
                Collections.shuffle(monitorIds);

                // Select d monitors to sample for this task
                myMonitors = new ArrayList<>();
                for(i = 0; i < d; i++){
                    myMonitors.add(monitorIds.get(d % monitorIds.size()));
                }

                // Create probe message, USING TASK ID IN THE JOB ID FIELD
                taskId = UUID.randomUUID();
                ProbeContent probe = new ProbeContent(taskId, this.id);
                Message probeMessage = new Message(MessageType.PROBE, probe);

                // Store task UUID -> job UUID in hashmap so that we can find the job later
                task2job.put(taskId, m.getJobID());
                // Ditto with the job spec
                task2spec.put(taskId, taskSpecs[i]);

                for(int monitorId : myMonitors){
                    log("sending task probe to monitor " + monitorId + "(taskId = " + taskId + ")");
                    objToNodeMonitors.get(monitorId).writeObject(probeMessage);
                }
            }


        }

        @Override
        public void receivedSpecRequest(ProbeReplyContent m) throws IOException {
            TaskSpecContent taskSpec;

            UUID jobId = task2job.get(m.getJobID());
            UUID taskId = m.getJobID();
            String taskSpecStr = task2spec.get(m.getJobID());

            // Identify the node monitor making the request
            int monitorId = m.getMonitorID();

            synchronized (takenTasks){
                if (takenTasks.contains(taskId)) {
                    log("recieved request for already-allocated task, sending null reply");
                    taskSpec = new TaskSpecContent(jobId, null, this.id, null);
                } else {
                    takenTasks.add(taskId);
                    log("received spec request from NodeMonitor, sending task " + taskId + " to NodeMonitor");
                    // Create one task spec to pass to node monitor
                    taskSpec = new TaskSpecContent(jobId, UUID.randomUUID(), this.id, taskSpecStr);
                }
            }
            // if task hasn't been given out, send the task spec to the first responder
            // and mark this task as already given (USING TASK ID IN JOB ID FIELD)

            // otherwise, reply with null response

            Message spec = new Message(MessageType.TASK_SPEC, taskSpec);
            objToNodeMonitors.get(monitorId).writeObject(spec);
        }


}
