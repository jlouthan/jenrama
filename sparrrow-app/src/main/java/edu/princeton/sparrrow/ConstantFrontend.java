package edu.princeton.sparrrow;

import org.json.JSONObject;

import java.net.Socket;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;

public class ConstantFrontend extends Frontend{
    private int jobTime;
    private int numJobs;
    private static int numTasksPerJob = 5;
    private Random r;

    public ConstantFrontend(int id, Socket socketWithSched, int numJobs, int jobTime){
        super(id, socketWithSched);
        this.numJobs = numJobs;
        this.jobTime = jobTime;
        r = new Random(id);
    }

    protected Collection<Collection<String>> makeJobs() {
        LinkedList<Collection<String>> allJobs = new LinkedList<>();

        JSONObject spec = new JSONObject();
        spec.put("Time", jobTime);
        LinkedList<String> myJob;
        for (int i = 0; i < numJobs; i++) {
            myJob = new LinkedList<>();
            for (int j = 0; j < numTasksPerJob; j++) {
                myJob.add(spec.toString());
            }
            allJobs.add(myJob);
        }

        log(allJobs + "");
        return allJobs;
    }

    protected Collection<Collection<String>> makeRandomJobs() {
        LinkedList<Collection<String>> allJobs = new LinkedList<>();

        JSONObject spec;
        LinkedList<String> myJob;
        for (int i = 0; i < numJobs; i++) {
            myJob = new LinkedList<>();
            for (int j = 0; i < numTasksPerJob; i++) {
                spec = new JSONObject();
                spec.put("Time", jobTime + r.nextInt(jobTime * 5));
                myJob.add(spec.toString());
            }
            allJobs.add(myJob);
        }

        return allJobs;
    }

    protected void handleResult(JobResultContent result){
        int i = 0;
        for(String taskResult : result.getResults()){
            JSONObject myResult = new JSONObject(taskResult);
            super.log("Task " + ++i);
            super.log("Elapsed time: " + myResult.getDouble("result") + " seconds");
        }
    }
}
