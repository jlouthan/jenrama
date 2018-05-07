package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.*;

public class PerTaskScheduler extends Scheduler {
        private HashMap<UUID, PendingTask> pendingTasks;

        public PerTaskScheduler(int id, PipedInputStream pipeFromFe, PipedOutputStream pipeToFe,
                               ArrayList<PipedInputStream> pipesFromNodeMonitor, ArrayList<PipedOutputStream> pipesToNodeMonitor, int d) {
            super(id, pipeFromFe, pipeToFe, pipesFromNodeMonitor, pipesToNodeMonitor, d);

            pendingTasks = new HashMap<>();
        }

        // Upon recieving a job, send out d probes for each task
        // PUTS THE TASK ID IN THE JOB ID FIELD
        @Override
        public synchronized void receivedJob(JobSpecContent m) throws IOException {
            log(id + " received job spec from Frontend, starting task sampling (jobId = " + m.getJobID() + ")");

            String[] taskSpecs = (String[]) m.getTasks().toArray();

            ArrayList<Integer> myMonitors;
            UUID taskId;
            PendingTask pt;
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

                // Store task UUID -> PendingTask in hashmap so that we can find the job later
                pt = new PendingTask(taskId, m.getJobID(), d, taskSpecs[i], myMonitors);
                pendingTasks.put(taskId, pt);

                for(int monitorId : myMonitors){
                    log("sending task probe to monitor " + monitorId + "(taskId = " + taskId + ")");
                    objToNodeMonitors.get(monitorId).writeObject(probeMessage);
                }
            }


        }

        @Override
        public synchronized void receivedSpecRequest(ProbeReplyContent m) throws IOException {
            TaskSpecContent taskSpec;
            DetailedProbeReplyContent mes = (DetailedProbeReplyContent) m;

            // Retrieve information for the task referred to in this probe
            PendingTask myTask = pendingTasks.get(m.getJobID());
            UUID jobId = myTask.myJob;
            UUID taskId = myTask.id;
            String taskSpecStr = myTask.spec;

            // Identify the node monitor making the request
            int monitorId = m.getMonitorID();

            // Store the value that the node monitor gave for its queue for this task
            myTask.replies[myTask.myMonitors.indexOf(monitorId)] = mes.getqLength();
            myTask.numReplies++;

            // If all our probes have returned, give the task to the best monitor
            if(myTask.numReplies == d){
                log("finished sampling for task" + taskId + ", sending to NodeMonitors");

                // Figure out the best monitor (shortest queue) to give my task to
                int i;
                int bestMonitor = -1;
                int bestQ = Integer.MAX_VALUE;
                for(i = 0; i < myTask.d; i++){
                    if (myTask.replies[i] < bestQ){
                        bestMonitor = myTask.myMonitors.get(i);
                    }
                }

                // Create task spec to pass to node monitor
                taskSpec = new TaskSpecContent(jobId, UUID.randomUUID(), this.id, taskSpecStr);

                // Send the task to my favorite monitor
                Message spec = new Message(MessageType.TASK_SPEC, taskSpec);
                objToNodeMonitors.get(bestMonitor).writeObject(spec);
            }
        }

        private class PendingTask {
            final UUID id;
            final UUID myJob;
            final int d;
            final int[] replies;
            final ArrayList<Integer> myMonitors;
            int numReplies = 0;
            final String spec;


            PendingTask(UUID id, UUID myJob, int d, String spec,  ArrayList<Integer> myMonitors){
                this.id = id;
                this.myJob = myJob;
                this.d = d;
                this.replies = new int[d];
                this.myMonitors = myMonitors;
                this.spec = spec;
            }

    }

}
