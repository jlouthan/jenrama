package edu.princeton.sparrrow;

import org.json.JSONObject;

import java.net.Socket;
import java.util.Collection;
import java.util.LinkedList;

public class RandstatFrontend extends Frontend {

    protected int numJobs;

    public RandstatFrontend(int id, Socket socketWithSched, int numJobs){
        super(id, socketWithSched);
        this.numJobs = numJobs;
    }

    protected Collection<Collection<String>> makeJobs() {
        LinkedList<Collection<String>> allJobs = new LinkedList<>();

        for (int i = 0; i < numJobs; i++) {
            allJobs.add(makeJob5());
        }

        return allJobs;
    }

    private Collection<String> makeJob1(){
        LinkedList<String> tasks = new LinkedList<>();

        JSONObject task1 = new JSONObject();
        task1.put("maxVal", 100);
        task1.put("num", 1000);
        task1.put("seed", 0);

        JSONObject task2 = new JSONObject();
        task2.put("maxVal", 10000);
        task2.put("num", 325);
        task2.put("seed", 10);

        JSONObject task3 = new JSONObject();
        task3.put("maxVal", 99999);
        task3.put("num", 500);
        task3.put("seed", 99);

        tasks.add(task1.toString());
        tasks.add(task2.toString());
        tasks.add(task3.toString());
        return tasks;
    }

    private Collection<String> makeJob2(){
        LinkedList<String> tasks = new LinkedList<>();

        JSONObject task1 = new JSONObject();
        task1.put("maxVal", 1234);
        task1.put("num", 23);
        task1.put("seed", 5);

        JSONObject task2= new JSONObject();
        task2.put("maxVal", 1235643);
        task2.put("num", 32);
        task2.put("seed", 365);

        tasks.add(task1.toString());
        tasks.add(task2.toString());
        return tasks;
    }


    private Collection<String> makeJob3(){
        LinkedList<String> tasks = new LinkedList<>();
        int n_tasks = 10;

        JSONObject task = new JSONObject();
        for(int i = 0; i < n_tasks; i++) {
            task.put("maxVal", 50);
            task.put("num", 500 + i);
            task.put("seed", 72 + i);

            tasks.add(task.toString());
        }

        return tasks;
    }

    // make 5 task job
    private Collection<String> makeJob5(){
        LinkedList<String> tasks = new LinkedList<>();
        int n_tasks = 5;

        JSONObject task = new JSONObject();
        for(int i = 0; i < n_tasks; i++) {
            task.put("maxVal", 50);
            task.put("num", 500 + i);
            task.put("seed", 72 + i);

            tasks.add(task.toString());
        }

        return tasks;
    }

    protected void handleResult(JobResultContent result){
        int i = 0;
        for(String taskResult : result.getResults()){
            JSONObject myResult = new JSONObject(taskResult);
            super.log("Task " + ++i);
            super.log("Count: " + myResult.getInt("count"));
            super.log("Sum: " + myResult.getInt("sum"));
            super.log("Mean: " + myResult.getInt("mean"));
        }
    }

}
