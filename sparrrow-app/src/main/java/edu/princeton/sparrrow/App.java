package edu.princeton.sparrrow;

/**
 * The App class launches the daemons.
 */

// import edu.princeton.sparrrow.Scheduler;

public class App {

    public static void main( String[] args ) {
        System.out.println("Starting Sparrrow App.");

        Scheduler s = new Scheduler();

        Thread t = new Thread(s, "sched");
        t.start();
    }
}
