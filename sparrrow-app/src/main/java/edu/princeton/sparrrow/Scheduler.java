package edu.princeton.sparrrow;

import java.io.*;

/**
 * The scheduler receives jobs from frontend instances and coordinates
 * between node monitors, placing probes and scheduling job tasks
 * according to the scheduling policy.
 */

public class Scheduler implements Runnable {

    private PipedInputStream pipe_i;
    private PipedOutputStream pipe_o;

    private ObjectInputStream obj_i;
    private ObjectOutputStream obj_o;

    public Scheduler(PipedInputStream pipe_i, PipedOutputStream pipe_o){
        this.pipe_i = pipe_i;
        this.pipe_o = pipe_o;
    }

    public void run() {
        String newJob = "unemployed";
        try {
            this.obj_i = new ObjectInputStream(pipe_i);

            // Receive job from Frontend
            newJob = ((Message) obj_i.readObject()).getBody();

            // switch(message.type

            // Handle message



            pipe_i.close();
            pipe_o.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println(newJob);


        for (int i = 0; i < 2; i++) {
            System.out.println("Scheduler iteration " + i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Finishing scheduler.");


    }

}
