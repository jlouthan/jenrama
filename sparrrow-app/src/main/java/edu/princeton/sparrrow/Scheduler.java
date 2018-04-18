package edu.princeton.sparrrow;

import java.io.*;

/**
 * The scheduler receives jobs from frontend instances and coordinates
 * between node monitors, placing probes and scheduling job tasks
 * according to the scheduling policy.
 */

public class Scheduler implements Runnable {
    // IO streams to and from Frontend
    private PipedInputStream pipe_from_fe;
    private PipedOutputStream pipe_to_fe;

    private ObjectInputStream obj_from_fe;
    private ObjectOutputStream obj_to_fe;

    // IO streams to and from NodeMonitor
    private PipedInputStream pipe_from_monitor;
    private PipedOutputStream pipe_to_monitor;

    private ObjectInputStream obj_from_monitor;
    private ObjectOutputStream obj_to_monitor;

    public Scheduler(PipedInputStream pipe_from_fe, PipedOutputStream pipe_to_fe,
                     PipedInputStream pipe_from_monitor, PipedOutputStream pipe_to_monitor){
        this.pipe_from_fe = pipe_from_fe;
        this.pipe_to_fe = pipe_to_fe;

        this.pipe_from_monitor = pipe_from_monitor;
        this.pipe_to_monitor = pipe_to_monitor;
    }

    public void run() {
        String newJob;
        String taskResult;

        try {

            // Set up object IO with Frontend
            this.obj_to_fe = new ObjectOutputStream(pipe_to_fe);
            this.obj_from_fe = new ObjectInputStream(pipe_from_fe);


            // Set up object IO with NodeMonitor
            this.obj_to_monitor = new ObjectOutputStream(pipe_to_monitor);
            this.obj_from_monitor = new ObjectInputStream(pipe_from_monitor);

            log("started");

            // Receive job from Frontend
            newJob = ((Message) obj_from_fe.readObject()).getBody();
            // Handle message
            receivedJob(newJob);

            // Receive task result from NodeMonitor
            taskResult = ((Message) obj_from_monitor.readObject()).getBody();
            // Handle message
            receivedResult(taskResult);

            // Close IO channels
            pipe_from_fe.close();
            pipe_to_fe.close();
            pipe_to_monitor.close();
            pipe_from_monitor.close();

            log("finishing");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void log(String text){
        System.out.println("Scheduler: " + text);
    }

    private void receivedJob(String m) throws IOException{
        //TODO: Store some state about the job

        //TODO: Send reservations to node monitors

        // For now, just pass it along to the NodeMonitor
        log("received job spec from Frontend, sending task spec to NodeMonitor");
        Message spec = new Message(MessageType.TASK_SPEC, m);
        obj_to_monitor.writeObject(spec);
    }

    private void receivedSpecRequest(String m) throws IOException{
        // TODO: reply to request with job specification
    }

    private void receivedResult(String m) throws IOException{
        // TODO: collect task result, return if it's done

        // For now, just pass it back up to the Frontend
        log("received task result from NodeMonitor, sending job result to Frontend");
        Message reply = new Message(MessageType.JOB_RESULT, m);
        obj_to_fe.writeObject(reply);
    }
}
