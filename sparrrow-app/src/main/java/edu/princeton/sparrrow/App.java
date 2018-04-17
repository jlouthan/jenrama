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

            PipedOutputStream pipe_o = new PipedOutputStream();
            PipedInputStream pipe_i = new PipedInputStream(pipe_o);

            ObjectOutputStream obj_o = new ObjectOutputStream(pipe_o);
            ObjectInputStream obj_i = new ObjectInputStream(pipe_i);
            
            Scheduler sched = new Scheduler(obj_o);
            Frontend f = new Frontend(obj_i);

            System.out.println("A");

            Thread schedulerThread = new Thread(sched, "sched");
            schedulerThread.start();

            System.out.println("B");


            Thread frontendThread = new Thread(f, "frontend");
            frontendThread.start();

            System.out.println("C");

        } catch (IOException e){
            System.out.println("oops");
        }

    }
}
