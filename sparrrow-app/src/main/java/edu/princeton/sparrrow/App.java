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
            PipedOutputStream pipeOutFeSched = new PipedOutputStream();
            PipedInputStream pipeInFeSched = new PipedInputStream(pipeOutFeSched);

            PipedOutputStream pipeOutSchedFe = new PipedOutputStream();
            PipedInputStream pipeInSchedFe = new PipedInputStream(pipeOutSchedFe);

            // Initialize streams between Scheduler and NodeMonitor
            PipedOutputStream pipeOutSchedMonitor = new PipedOutputStream();
            PipedInputStream pipeInSchedMonitor = new PipedInputStream(pipeOutSchedMonitor);

            PipedOutputStream pipeOutMonitorSched = new PipedOutputStream();
            PipedInputStream pipeInMonitorSched = new PipedInputStream(pipeOutMonitorSched);

            // Initialize streams betwee NodeMonitor and Executor
            PipedOutputStream pipeOutMonitorExec = new PipedOutputStream();
            PipedInputStream pipeInMonitorExec = new PipedInputStream(pipeOutMonitorExec);

            // Initialize streams between Executor and NodeMonitor
            PipedOutputStream pipeOutExecMonitor = new PipedOutputStream();
            PipedInputStream pipeInExecMonitor = new PipedInputStream(pipeOutExecMonitor);


            // Initialize our actors - Frontend, Scheduler, NodeMonitor, and Executor
            Frontend f = new RandstatFrontend(0, pipeInSchedFe, pipeOutFeSched);
            Scheduler sched = new Scheduler(0, pipeInFeSched, pipeOutSchedFe,
                    pipeInMonitorSched, pipeOutSchedMonitor);
            NodeMonitor monitor = new NodeMonitor(0, pipeInSchedMonitor, pipeOutMonitorSched,
                    pipeInExecMonitor, pipeOutMonitorExec);
            Executor exec = new RandstatExecutor(0, pipeInMonitorExec, pipeOutExecMonitor);

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
