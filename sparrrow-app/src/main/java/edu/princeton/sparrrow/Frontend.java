package edu.princeton.sparrrow;

import java.io.*;

/**
 * A frontend submits jobs to schedulers and receive results in return.
 */

public class Frontend implements Runnable {

    private PipedInputStream pipe_i;
    private PipedOutputStream pipe_o;

    private ObjectInputStream obj_i;
    private ObjectOutputStream obj_o;

    public Frontend(PipedInputStream pipe_i, PipedOutputStream pipe_o){
        this.pipe_i = pipe_i;
        this.pipe_o = pipe_o;
    }

    public void run() {
        try {
            // Generate job to run

            String job = "please run me :)";

            // Send to scheduler, await result
            try {
                this.obj_o = new ObjectOutputStream(pipe_o);
                obj_o.writeObject(job);

                pipe_i.close();
                pipe_o.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Print result


            for (int i = 0; i < 2; i++) {
                System.out.println("Frontend iteration " + i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("Frontend thread interrupted.");
        }
        System.out.println("Finishing frontend.");
    }
}
