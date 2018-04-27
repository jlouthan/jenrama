package edu.princeton.sparrrow;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;

/**
 * The App class launches the daemons.
 */

// import edu.princeton.sparrrow.Scheduler;

public class App {

    public static void main( String[] args ) {
        System.out.println("Starting Sparrrow App.");

        try{

            int n_frontends = 3;
            int n_executors = 5;
            int i;

            ArrayList<Scheduler> schedulers = new ArrayList<>();
            ArrayList<Frontend> fes = new ArrayList<>();
            ArrayList<NodeMonitor> monitors = new ArrayList<>();
            ArrayList<Executor> executors = new ArrayList<>();


            // Initialize streams between Scheduler and NodeMonitor
            ArrayList<PipedInputStream> monitorsIn = new ArrayList<>();
            ArrayList<PipedInputStream> schedsIn = new ArrayList<>();
            ArrayList<PipedOutputStream> monitorsOut = new ArrayList<>();
            ArrayList<PipedOutputStream> schedsOut = new ArrayList<>();


            PipedOutputStream pipeOutSchedMonitor;
            PipedInputStream pipeInSchedMonitor;
            PipedOutputStream pipeOutMonitorSched;
            PipedInputStream pipeInMonitorSched;

            // nf * ne total pipes
            // the pipe between scheduler i and monitor j is at index i*(ne) + j - 1
            // (not that we'll ever need to access them this way hopefully)
            for (i = 0; i < n_frontends * n_executors; i++){
                pipeOutSchedMonitor = new PipedOutputStream();
                pipeInSchedMonitor = new PipedInputStream(pipeOutSchedMonitor);

                pipeOutMonitorSched = new PipedOutputStream();
                pipeInMonitorSched = new PipedInputStream(pipeOutMonitorSched);

                monitorsIn.add(pipeInSchedMonitor);
                monitorsOut.add(pipeOutMonitorSched);

                schedsIn.add(pipeInMonitorSched);
                schedsOut.add(pipeOutSchedMonitor);
            }

            // Initialize streams between Frontend and Scheduler
            // and create Frontends and Schedulers at the same time
            PipedOutputStream pipeOutFeSched;
            PipedInputStream pipeInFeSched;

            PipedOutputStream pipeOutSchedFe;
            PipedInputStream pipeInSchedFe;

            Frontend fe;
            Scheduler sched;
            for (i = 0; i < n_frontends; i++){
                pipeOutFeSched = new PipedOutputStream();
                pipeInFeSched = new PipedInputStream(pipeOutFeSched);

                pipeOutSchedFe = new PipedOutputStream();
                pipeInSchedFe = new PipedInputStream(pipeOutSchedFe);

                fe = new RandstatFrontend(i, pipeInSchedFe, pipeOutFeSched);
                fes.add(fe);

                sched = new Scheduler(i, pipeInFeSched, pipeOutSchedFe,
                        schedsIn, schedsOut);
                schedulers.add(sched);
            }



            // Initialize streams between NodeMonitor and Executor
            // and create Executors and Nodemonitors at the same time
            PipedOutputStream pipeOutMonitorExec = new PipedOutputStream();
            PipedInputStream pipeInMonitorExec = new PipedInputStream(pipeOutMonitorExec);

            PipedOutputStream pipeOutExecMonitor = new PipedOutputStream();
            PipedInputStream pipeInExecMonitor = new PipedInputStream(pipeOutExecMonitor);

            Executor ex;
            for(i = 0; i < n_executors; i++){
                pipeOutMonitorExec = new PipedOutputStream();
                pipeInMonitorExec = new PipedInputStream(pipeOutMonitorExec);

                // Initialize streams between Executor and NodeMonitor
                pipeOutExecMonitor = new PipedOutputStream();
                pipeInExecMonitor = new PipedInputStream(pipeOutExecMonitor);

                ex = new RandstatExecutor(i, pipeInMonitorExec, pipeOutExecMonitor);
                executors.add(ex);

                NodeMonitor monitor = new NodeMonitor(i, monitorsIn, monitorsOut,
                        pipeInExecMonitor, pipeOutMonitorExec);
                monitors.add(monitor);
            }


            ArrayList<Thread> allThreads = new ArrayList<>();
            Thread t;

            // Start Executors
            for(i = 0; i < executors.size(); i++){
                t = new Thread(executors.get(i), "Executor " + i);
                t.start();
                allThreads.add(t);
            }

            // Start NodeMonitors
            for(i = 0; i < monitors.size(); i++){
                t = new Thread(monitors.get(i), "Monitor " + i);
                t.start();
                allThreads.add(t);
            }

            // Start Schedulers
            for(i = 0; i < schedulers.size(); i++){
                t = new Thread(schedulers.get(i), "Scheduler " + i);
                t.start();
                allThreads.add(t);
            }

            // Start Frontends
            for(i = 0; i < fes.size(); i++){
                t = new Thread(fes.get(i), "Frontend " + i);
                t.start();
                allThreads.add(t);
            }

            // Await all threads finishing (unused for now)
            for(Thread th: allThreads){
                //th.join();
            }

        } catch (IOException e){
            System.out.println("oops");
        }

    }
}
