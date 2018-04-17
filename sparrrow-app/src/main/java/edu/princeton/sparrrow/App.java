package edu.princeton.sparrrow;

import java.io.*;

/**
 * The App class launches the daemons.
 */

// import edu.princeton.sparrrow.Scheduler;

public class App {

    public static void main( String[] args ) {
        System.out.println("Starting Sparrrow App.");

        try{
            // Initialize stream from Frontend to Scheduler
            PipedOutputStream pipe_o_fe_sched = new PipedOutputStream();
            PipedInputStream pipe_i_fe_sched = new PipedInputStream(pipe_o_fe_sched);

            // Initialize stream from Scheduler to Frontend
            PipedOutputStream pipe_o_sched_fe = new PipedOutputStream();
            PipedInputStream pipe_i_sched_fe = new PipedInputStream(pipe_o_sched_fe);

            Frontend f = new Frontend(pipe_i_sched_fe, pipe_o_fe_sched);
            Scheduler sched = new Scheduler(pipe_i_fe_sched, pipe_o_sched_fe);

            Thread schedulerThread = new Thread(sched, "sched");
            Thread frontendThread = new Thread(f, "frontend");
            frontendThread.start();
            schedulerThread.start();

        } catch (IOException e){
            System.out.println("oops");
        }

    }
}
