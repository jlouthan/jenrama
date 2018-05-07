package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

public class BatchScheduler extends Scheduler {
    private HashMap<UUID, Job> pendingJobs;

    public BatchScheduler(int id, PipedInputStream pipeFromFe, PipedOutputStream pipeToFe,
                          ArrayList<PipedInputStream> pipesFromNodeMonitor, ArrayList<PipedOutputStream> pipesToNodeMonitor, int d) {
        super(id, pipeFromFe, pipeToFe, pipesFromNodeMonitor, pipesToNodeMonitor, d);

        pendingJobs = new HashMap<>();
    }

    public synchronized void receivedSpecRequest(DetailedProbeReplyContent m) throws IOException {

    }

}
