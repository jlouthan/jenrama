package edu.princeton.sparrrow;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

public class RandomScheduler extends Scheduler {
    public RandomScheduler(int id, ServerSocket socketWithFe, ArrayList<Socket> socketsWithMonitors, int d) throws IOException {
        super(id, socketWithFe, socketsWithMonitors, d);
    }

    @Override
    public synchronized void receivedJob(JobSpecContent m) throws IOException {
        Job j = new Job(m.getFrontendID(), m.getTasks());
        jobs.put(m.getJobID(), j);

        log(id + " received job spec from Frontend, distributing tasks randomly");

        String taskSpecStr;
        TaskSpecContent taskContent;
        UUID taskId;
        Message task;
        int myMonitor;
        while((taskSpecStr = j.getNextTaskRemaining()) != null){
            // Select a random monitor from my list of monitors
            Collections.shuffle(monitorIds);
            myMonitor = monitorIds.get(0);

            // Make and send a task spec message to the node monitor
            taskId = UUID.randomUUID();
            taskContent = new TaskSpecContent(m.getJobID(), taskId, id, taskSpecStr);
            task = new Message(MessageType.TASK_SPEC, taskContent);
            objToNodeMonitors.get(myMonitor).writeObject(task);

            log("sending task (" + taskId + ") to NodeMonitor " + myMonitor);

            // increment stats for specs
            j.specStats.incrementCount(myMonitor);
        }


    }


    @Override
    public synchronized void receivedSpecRequest(ProbeReplyContent m) throws IOException {
        log("ERROR: RandomScheduler sending specs instead of probes, but I recieved a spec request");
    }
}
