package edu.princeton.sparrrow;

import java.io.ObjectOutputStream;

/**
 * The scheduler receives jobs from frontend instances and coordinates
 * between node monitors, placing probes and scheduling job tasks
 * according to the scheduling policy.
 */

public class Scheduler implements Runnable {

    private ObjectOutputStream obj_o;

    public Scheduler(ObjectOutputStream obj_o) {
        this.obj_o = obj_o;
    }

    public void run() {
        try {
            for (int i = 0; i < 4; i++) {
                System.out.println("Scheduler iteration " + i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("Scheduler thread interrupted.");
        }
        System.out.println("Finishing scheduler.");
    }

}
