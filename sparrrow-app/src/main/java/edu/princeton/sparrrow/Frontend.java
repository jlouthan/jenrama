package edu.princeton.sparrrow;

import java.io.*;

/**
 * A frontend submits jobs to schedulers and receive results in return.
 */

public class Frontend implements Runnable {

    private PipedInputStream pipe_from_sched;
    private PipedOutputStream pipe_to_sched;

    private ObjectInputStream obj_from_sched;
    private ObjectOutputStream obj_to_sched;

    public Frontend(PipedInputStream pipe_from_sched, PipedOutputStream pipe_to_sched){
        this.pipe_from_sched = pipe_from_sched;
        this.pipe_to_sched = pipe_to_sched;
    }

    public void run() {
        String result = "no result";
        String job = "please run me :)";
        try {
            // Set up IO streams with Scheduler
            this.obj_to_sched = new ObjectOutputStream(pipe_to_sched);
            this.obj_from_sched = new ObjectInputStream(pipe_from_sched);

            log("started");

            // Send job specification to scheduler, await result
            Message m = new Message(MessageType.JOB_SPEC, job);
            log("sending job spec to scheduler");
            obj_to_sched.writeObject(m);
            obj_to_sched.flush();

            // Print result
            result = ((Message) obj_from_sched.readObject()).getBody();
            log("received result: " + result);

            pipe_from_sched.close();
            pipe_to_sched.close();
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
