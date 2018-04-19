package edu.princeton.sparrrow;

import java.io.*;

/**
 * The scheduler receives jobs from frontend instances and coordinates
 * between node monitors, placing probes and scheduling job tasks
 * according to the scheduling policy.
 */

public class Scheduler implements Runnable {
    // IO streams to and from Frontend
    private PipedInputStream pipeFromFe;
    private PipedOutputStream pipeToFe;

    private ObjectInputStream objFromFe;
    private ObjectOutputStream objToFe;

    // IO streams to and from NodeMonitor
    private PipedInputStream pipeFromNodeMonitor;
    private PipedOutputStream pipeToNodeMonitor;

    private ObjectInputStream objFromMonitor;
    private ObjectOutputStream objToMonitor;

    public Scheduler(PipedInputStream pipeFromFe, PipedOutputStream pipeToFe,
                     PipedInputStream pipeFromNodeMonitor, PipedOutputStream pipeToNodeMonitor){
        this.pipeFromFe = pipeFromFe;
        this.pipeToFe = pipeToFe;

        this.pipeFromNodeMonitor = pipeFromNodeMonitor;
        this.pipeToNodeMonitor = pipeToNodeMonitor;
    }

    public void run() {
        String newJob;
        String taskResult;

        try {

            // Set up object IO with Frontend
            this.objToFe = new ObjectOutputStream(pipeToFe);
            this.objFromFe = new ObjectInputStream(pipeFromFe);


            // Set up object IO with NodeMonitor
            this.objToMonitor = new ObjectOutputStream(pipeToNodeMonitor);
            this.objFromMonitor = new ObjectInputStream(pipeFromNodeMonitor);

            log("started");

            // Receive job from Frontend
            newJob = ((Message) objFromFe.readObject()).getBody();
            // Handle message
            receivedJob(newJob);

            // Receive task result from NodeMonitor
            taskResult = ((Message) objFromMonitor.readObject()).getBody();
            // Handle message
            receivedResult(taskResult);

            // Close IO channels
            pipeFromFe.close();
            pipeToFe.close();
            pipeToNodeMonitor.close();
            pipeFromNodeMonitor.close();

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
        objToMonitor.writeObject(spec);
    }

    private void receivedSpecRequest(String m) throws IOException{
        // TODO: reply to request with job specification
    }

    private void receivedResult(String m) throws IOException{
        // TODO: collect task result, return if it's done

        // For now, just pass it back up to the Frontend
        log("received task result from NodeMonitor, sending job result to Frontend");
        Message reply = new Message(MessageType.JOB_RESULT, m);
        objToFe.writeObject(reply);
    }
}
