package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/**
 * The node monitor receives probes from schedulers, communicates with schedulers
 * to coordinate receiving job tasks, and sends those tasks to its executor.
 */

public class EagerNodeMonitor extends NodeMonitor {
    private Queue<TaskSpecContent> taskQueue;

    public EagerNodeMonitor(int id, ArrayList<ServerSocket> socketsWithScheds, Socket socketWithExec) throws IOException {
        super(id, socketsWithScheds, socketWithExec);
        this.taskQueue = new LinkedList<>();
    }

    private void sendDetailedProbeReply(ProbeContent pc, int queue_len) throws IOException {
        // Determine correct scheduler to write to
        int destination_scheduler = pc.getSchedID();
        ObjectOutputStream objToSched = this.objsToScheds.get(destination_scheduler);

        log("sending probe reply for job " + pc.getJobID() + " to scheduler " + destination_scheduler);

        // Send probe reply with queue length to scheduler
        DetailedProbeReplyContent probeReply = new DetailedProbeReplyContent(pc.getJobID(), this.id, queue_len);
        Message m = new Message(MessageType.DET_PROBE_REPLY, probeReply);
        objToSched.writeObject(m);
    }

    @Override
    public synchronized void handleProbe(ProbeContent pc) throws IOException{
        log("eager node monitor received probe from scheduler " + pc.getSchedID());

        // Get execution queue length
        int queue_len = taskQueue.size();

        // Send probe reply with length
        sendDetailedProbeReply(pc, queue_len);
    }

    private void queueTask(TaskSpecContent spec) throws IOException{
        log("adding task spec to queue");

        taskQueue.add(spec);
    }

    private void sendTaskToExecutor(TaskSpecContent spec) throws IOException {
        // Send spec to executor for execution
        log("sending task " + spec.getSpec() + " to executor");
        Message m = new Message(MessageType.TASK_SPEC, spec);
        objToExec.writeObject(m);

        // Mark executor as occupied
        executor_is_occupied = true;
    }

    private void sendNextTask() throws IOException {
        // Get next task
        TaskSpecContent spec = taskQueue.poll();
        if (spec != null) {
            sendTaskToExecutor(spec);
        } else {
            log("no more specs to send");
        }
    }

    @Override
    public synchronized void handleTaskSpec(TaskSpecContent spec) throws IOException{
        log("received task spec message from scheduler");
        // Queue spec if executor is occupied
        if (executor_is_occupied) {
            queueTask(spec);
        } else {
            // Start task if executor is free
            sendTaskToExecutor(spec);
        }
    }

    @Override
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
        sendNextTask();
    }

}
