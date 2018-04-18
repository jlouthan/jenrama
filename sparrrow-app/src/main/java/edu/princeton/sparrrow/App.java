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
            // Initialize streams between Frontend and Scheduler
            PipedOutputStream pipe_o_fe_sched = new PipedOutputStream();
            PipedInputStream pipe_i_fe_sched = new PipedInputStream(pipe_o_fe_sched);

            PipedOutputStream pipe_o_sched_fe = new PipedOutputStream();
            PipedInputStream pipe_i_sched_fe = new PipedInputStream(pipe_o_sched_fe);

            // Initialize streams between Scheduler and NodeMonitor
            PipedOutputStream pipe_o_sched_monitor = new PipedOutputStream();
            PipedInputStream pipe_i_sched_monitor = new PipedInputStream(pipe_o_sched_monitor);

            PipedOutputStream pipe_o_monitor_sched = new PipedOutputStream();
            PipedInputStream pipe_i_monitor_sched = new PipedInputStream(pipe_o_monitor_sched);

            // Initialize streams betwee NodeMonitor and Executor
            PipedOutputStream pipe_o_monitor_exec = new PipedOutputStream();
            PipedInputStream pipe_i_monitor_exec = new PipedInputStream(pipe_o_monitor_exec);

            // Initialize streams between Executor and NodeMonitor
            PipedOutputStream pipe_o_exec_monitor = new PipedOutputStream();
            PipedInputStream pipe_i_exec_monitor = new PipedInputStream(pipe_o_exec_monitor);


            // Initialize our actors - Frontend, Scheduler, NodeMonitor, and Executor
            Frontend f = new Frontend(pipe_i_sched_fe, pipe_o_fe_sched);
            Scheduler sched = new Scheduler(pipe_i_fe_sched, pipe_o_sched_fe,
                    pipe_i_monitor_sched, pipe_o_sched_monitor);
            NodeMonitor monitor = new NodeMonitor(pipe_i_sched_monitor, pipe_o_monitor_sched,
                    pipe_i_exec_monitor, pipe_o_monitor_exec);
            Executor exec = new Executor(pipe_i_monitor_exec, pipe_o_exec_monitor);

            Thread schedulerThread = new Thread(sched, "sched");
            Thread frontendThread = new Thread(f, "frontend");
            Thread monitorThread = new Thread(monitor, "monitor");
            Thread execThread = new Thread(exec, "exec");

            execThread.start();
            monitorThread.start();
            schedulerThread.start();
            frontendThread.start();

        } catch (IOException e){
            System.out.println("oops");
        }

    }
}
