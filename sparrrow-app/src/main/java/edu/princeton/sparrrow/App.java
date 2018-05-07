package edu.princeton.sparrrow;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * The App class launches the daemons.
 */

// import edu.princeton.sparrrow.Scheduler;

public class App {

    public static void main( String[] args ) {
        System.out.println("Starting Sparrrow App.");

        try{

            int n_frontends = 2;
            int n_executors = 3;
            int i;

            int port0 = 32000;
            int portCounter = 0;


            ArrayList<Scheduler> schedulers = new ArrayList<>();
            ArrayList<Frontend> fes = new ArrayList<>();
            ArrayList<NodeMonitor> monitors = new ArrayList<>();
            ArrayList<Executor> executors = new ArrayList<>();


            // Initialize streams between Scheduler and NodeMonitor
            ArrayList<Socket> schedSocketsToMonitor = new ArrayList<>();
            ArrayList<ServerSocket> monitorSocketsToSched  = new ArrayList<>();

            ServerSocket monitorSocket;
            Socket schedSocket;

            // nf * ne total pipes
            // the pipe between scheduler i and monitor j is at index i*(ne) + j - 1
            for (i = 0; i < n_frontends * n_executors; i++){
                monitorSocket = new ServerSocket(port0 + portCounter);
                schedSocket = new Socket("127.0.0.1", port0 + portCounter++);

                schedSocketsToMonitor.add(schedSocket);
                monitorSocketsToSched.add(monitorSocket);
            }

            // Initialize streams between Frontend and Scheduler
            // and create Frontends and Schedulers at the same time

            ServerSocket schedSocketWithFe;
            Socket feSocketWithSched;

            ArrayList<Socket> mySocketsWithMonitor;
            int j;

            Frontend fe;
            Scheduler sched;
            for (i = 0; i < n_frontends; i++){

                schedSocketWithFe = new ServerSocket(port0 + portCounter);
                feSocketWithSched = new Socket("127.0.0.1", port0 + portCounter++);

                fe = new RandstatFrontend(i, feSocketWithSched);
                fes.add(fe);

                mySocketsWithMonitor = new ArrayList<>();
                for(j = 0; j < n_executors; j++){
                    mySocketsWithMonitor.add(schedSocketsToMonitor.get(i * n_executors + j));
                }

                sched = new Scheduler(i, schedSocketWithFe, mySocketsWithMonitor, 2);
                schedulers.add(sched);
            }



            // Initialize streams between NodeMonitor and Executor
            // and create Executors and Nodemonitors at the same time

            Socket monitorSocketWithExec;
            ServerSocket execSocketWithMonitor;

            ArrayList<ServerSocket> mySocketsWithSched;

            Executor ex;
            for(i = 0; i < n_executors; i++){
                execSocketWithMonitor = new ServerSocket(port0 + portCounter);
                monitorSocketWithExec = new Socket("127.0.0.1", port0 + portCounter++);

                ex = new RandstatExecutor(i, execSocketWithMonitor);
                executors.add(ex);

                mySocketsWithSched = new ArrayList<>();
                for(j = 0; j < n_frontends; j++){
                    mySocketsWithSched.add(monitorSocketsToSched.get(i + n_executors * j));
                }

                NodeMonitor monitor = new NodeMonitor(i, mySocketsWithSched, monitorSocketWithExec);
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
            e.printStackTrace();
            System.out.println("App broke");
        }

    }
}
