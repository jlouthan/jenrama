package edu.princeton.sparrrow;

import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.UUID;

/**
 * A frontend submits jobs to schedulers and receive results in return.
 */

public class Frontend implements Runnable {

    protected final int id;

    private PipedInputStream pipeFromSched;
    private PipedOutputStream pipeToSched;

    private ObjectInputStream objFromSched;
    private ObjectOutputStream objToSched;

    public Frontend(int id, PipedInputStream pipeFromSched, PipedOutputStream pipeToSched){
        this.id = id;
        this.pipeFromSched = pipeFromSched;
        this.pipeToSched = pipeToSched;
    }

    public void run() {
        try {
            // Set up IO streams with Scheduler
            this.objToSched = new ObjectOutputStream(pipeToSched);
            this.objFromSched = new ObjectInputStream(pipeFromSched);

            log("started");


            //TODO: put this in a loop to send multiple jobs
            // Send job specification to scheduler, await result
            Message m = new Message(MessageType.JOB_SPEC, makeJob());

            log("sending job spec to scheduler");
            objToSched.writeObject(m);
            objToSched.flush();

            //TODO: put this in a loop to await jobs (need to keep track of remaining jobs)
            // Handle result
            JobResultContent resultContent = (JobResultContent)((Message) objFromSched.readObject()).getBody();
            handleResult(resultContent);


            pipeFromSched.close();
            pipeToSched.close();
            log("finishing");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected void log(String text){
        System.out.println("Frontend: " + text);
    }

    protected JobSpecContent makeJob(){
        ArrayList<String> tasks = new ArrayList<String>();

        JobSpecContent job = new JobSpecContent(UUID.randomUUID(), this.id, tasks);
        return job;
    }

    protected void handleResult(JobResultContent jobResult){
        log("received results from " + jobResult.getResults().size() + " tasks");
    }

}
