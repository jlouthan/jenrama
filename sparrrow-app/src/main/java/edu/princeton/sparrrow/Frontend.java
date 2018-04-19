package edu.princeton.sparrrow;

import java.io.*;

/**
 * A frontend submits jobs to schedulers and receive results in return.
 */

public class Frontend implements Runnable {

    private PipedInputStream pipeFromSched;
    private PipedOutputStream pipeToSched;

    private ObjectInputStream objFromSched;
    private ObjectOutputStream objToSched;

    public Frontend(PipedInputStream pipeFromSched, PipedOutputStream pipeToSched){
        this.pipeFromSched = pipeFromSched;
        this.pipeToSched = pipeToSched;
    }

    public void run() {
        String result = "no result";
        String job = "please run me :)";
        try {
            // Set up IO streams with Scheduler
            this.objToSched = new ObjectOutputStream(pipeToSched);
            this.objFromSched = new ObjectInputStream(pipeFromSched);

            log("started");

            // Send job specification to scheduler, await result
            Message m = new Message(MessageType.JOB_SPEC, job);
            log("sending job spec to scheduler");
            objToSched.writeObject(m);
            objToSched.flush();

            // Print result
            result = ((Message) objFromSched.readObject()).getBody();
            log("received result: " + result);

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
