package edu.princeton.sparrrow;

/**
 * The App class launches the daemons.
 */

// import edu.princeton.sparrrow.Scheduler;

public class App {

    public static void main( String[] args ) {
        System.out.println("Starting Sparrrow App.");

        PipedInputStream pipe_i = new PipedInputStream();
        PipedOutputStream pipe_o = new PipedOutputStream(pipe_i);

        ObjectInputStream obj_i = new ObjectInputStream(pipe_i);
        ObjectOutputStream obj_o = new ObjectOutputStream(pipe_o);

        Scheduler sched = new Scheduler(obj_o);
        Frontend f = new Frontend(obj_i);

        Thread schedulerThread = new Thread(sched, "sched");
        schedulerThread.start();

        Thread frontendThread = new Thread(fe, "frontend");
        frontendThread.start();
    }
}
