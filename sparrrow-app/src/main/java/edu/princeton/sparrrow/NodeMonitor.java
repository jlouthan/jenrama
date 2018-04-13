package edu.princeton.sparrrow;

/**
 * The node monitor receives probes from schedulers, communicates with schedulers
 * to coordinate receiving job tasks, and sends those tasks to its executor.
 */

public class NodeMonitor implements Runnable {

    public void run() {
        try {
            for (int i = 0; i < 4; i++) {
                System.out.println("NodeMonitor iteration " + i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("NodeMonitor thread interrupted.");
        }
        System.out.println("Finishing NodeMonitor.");
    }
}
