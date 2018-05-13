package edu.princeton.sparrrow;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * A frontend submits jobs to schedulers and receive results in return.
 */

public abstract class Frontend implements Runnable, Logger {

    protected final int id;
    private int count;

    private Socket socketWithSched;

    private ObjectInputStream objFromSched;
    private ObjectOutputStream objToSched;

    private HashSet<UUID> pendingJobs = new HashSet();

    public Frontend(int id, Socket socketWithSched){
        count = 0;
        this.id = id;
        this.socketWithSched = socketWithSched;
    }

    public void run() {
        try {

            // Set up IO streams with Scheduler
            this.objToSched = new ObjectOutputStream(socketWithSched.getOutputStream());
            this.objFromSched = new ObjectInputStream(socketWithSched.getInputStream());

            log("started");

            UUID job_id;
            JobSpecContent j;
            Message m;
            for(Collection<String> jobspec : makeJobs()){
                // Make job specification
                job_id = UUID.randomUUID();
                j = new JobSpecContent(job_id, this.id, jobspec);
                m = new Message(MessageType.JOB_SPEC, j);


                // Send job specifications to scheduler
                log("sending job spec to scheduler (ID = " + job_id.toString() + ")");
                objToSched.writeObject(m);

                // Add job id to pendingJobs
                pendingJobs.add(job_id);
            }

            // Awaits the completion of all jobs before exiting
            JobResultContent resultContent;
            try {
                while (!pendingJobs.isEmpty()) {
                    resultContent = (JobResultContent) ((Message) objFromSched.readObject()).getBody();

                    handleResult(resultContent);

                    pendingJobs.remove(resultContent.getJobID());

                    log("Received result for job " + count++ + " , awaiting " + pendingJobs.size() + " more");
                }
            } catch (ClassCastException e){
                log("ERROR: expected a JobResultContent message and got something else");
            }

            log("finished all tasks, closing down");
            DoneContent dc = new DoneContent(id);
            objToSched.writeObject(new Message(MessageType.DONE, dc));

            boolean acknowledged = false;
            try {
                while(!acknowledged){
                    DoneAckContent ack = (DoneAckContent) ((Message) objFromSched.readObject()).getBody();
                    if (ack.getId() == id){
                        acknowledged = true;
                    } else {
                        log("ERROR: received ack from scheduler " + ack.getId());
                    }
                }

            } catch (EOFException e){
                log("Exiting because my socket was closed");
            }
            socketWithSched.close();
            log("Exiting gracefully");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized void log(String text){
        System.out.println("Frontend[" + this.id + "]: " + text);
    }

    protected abstract Collection<Collection<String>> makeJobs();

    protected abstract void handleResult(JobResultContent jobResult);

}
