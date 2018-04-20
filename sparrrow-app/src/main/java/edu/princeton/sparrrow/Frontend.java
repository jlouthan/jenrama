package edu.princeton.sparrrow;

import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.UUID;

/**
 * A frontend submits jobs to schedulers and receive results in return.
 */

public class Frontend implements Runnable {

    private final int id;

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

            // Construct job
            ArrayList<String> tasks = new ArrayList<String>();
            tasks.add("");
            JobSpecContent job = new JobSpecContent(UUID.randomUUID(), this.id, tasks);

            // Send job specification to scheduler, await result
            Message m = new Message(MessageType.JOB_SPEC, job);
            log("sending job spec to scheduler");
            objToSched.writeObject(m);
            objToSched.flush();

            // Print result
            JobResultContent resultContent = (JobResultContent)((Message) objFromSched.readObject()).getBody();
            log("received results from " + resultContent.getResults().size() + " tasks");

            pipeFromSched.close();
            pipeToSched.close();
            log("finishing");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void log(String text){
        System.out.println("Frontend: " + text);
    }
}
