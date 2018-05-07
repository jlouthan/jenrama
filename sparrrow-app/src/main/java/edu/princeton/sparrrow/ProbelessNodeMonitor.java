package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/**
 * The node monitor receives probes from schedulers, communicates with schedulers
 * to coordinate receiving job tasks, and sends those tasks to its executor.
 */

public class ProbelessNodeMonitor extends NodeMonitor {
    private Queue<TaskSpecContent> taskQueue;

    public ProbelessNodeMonitor(int id, ArrayList<PipedInputStream> pipesFromScheds, ArrayList<PipedOutputStream> pipesToScheds,
                                PipedInputStream pipeFromExec, PipedOutputStream pipeToExec) throws IOException {
        super(id, pipesFromScheds, pipesToScheds, pipeFromExec, pipeToExec);
        this.taskQueue = new LinkedList<>();
    }

    @Override
    public synchronized void handleProbe(ProbeContent pc) throws IOException{
        log("ERROR: probeless node monitor received probe from scheduler " + pc.getSchedID());
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
        TaskSpecContent spec = taskQueue.peek();
        if (spec != null) {
            sendTaskToExecutor(spec);
        } else {
            log("no more specs to send");
        }
    }

    @Override
    public synchronized void handleTaskSpec(TaskSpecContent spec) throws IOException{
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
