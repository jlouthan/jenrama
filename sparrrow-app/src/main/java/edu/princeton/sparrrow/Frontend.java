package edu.princeton.sparrrow;

/**
 * A frontend submits jobs to schedulers and receive results in return.
 */

public class Frontend implements Runnable {

    ObjectInputStream obj_i;

    public void run() {
        try {
            // Generate job to run

            // Send to scheduler, await result

            // Print result


            for (int i = 0; i < 4; i++) {
                System.out.println("Frontend iteration " + i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("Frontend thread interrupted.");
        }
        System.out.println("Finishing frontend.");
    }
}
