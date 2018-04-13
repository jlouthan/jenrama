package edu.princeton.sparrrow;

/**
 * The executor receives a job task from a node monitor, executes it, and returns the results.
 */

public class Executor implements Runnable {

    public void run() {
        try {
            for (int i = 0; i < 4; i++) {
                System.out.println("Executor iteration " + i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("Executor thread interrupted.");
        }
        System.out.println("Finishing executor.");
    }
}
