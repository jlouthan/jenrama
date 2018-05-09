package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;

/**
 * A frontend submits jobs to schedulers and receive results in return.
 */

public abstract class Frontend implements Runnable {

    protected final int id;

    private Socket socketWithSched;

    private ObjectInputStream objFromSched;
    private ObjectOutputStream objToSched;

    private HashSet<UUID> pendingJobs = new HashSet();

    public Frontend(int id, Socket socketWithSched){
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
            while(!pendingJobs.isEmpty()){
                resultContent = (JobResultContent)((Message) objFromSched.readObject()).getBody();

                handleResult(resultContent);

                pendingJobs.remove(resultContent.getJobID());
            }

            while (true) ;

//            pipeFromSched.close();
//            pipeToSched.close();
//            log("finishing");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected void log(String text){
        System.out.println("Frontend[" + this.id + "]: " + text);
    }

    protected abstract Collection<Collection<String>> makeJobs();

    protected abstract void handleResult(JobResultContent jobResult);

}
