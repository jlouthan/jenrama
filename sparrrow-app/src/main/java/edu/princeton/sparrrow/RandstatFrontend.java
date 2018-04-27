package edu.princeton.sparrrow;

import org.json.JSONObject;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.UUID;

public class RandstatFrontend extends Frontend {

    public RandstatFrontend(int id, PipedInputStream pipeFromSched, PipedOutputStream pipeToSched){
        super(id, pipeFromSched, pipeToSched);
    }

    @Override
    protected JobSpecContent makeJob() {
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

        JobSpecContent job = new JobSpecContent(UUID.randomUUID(), super.id, tasks);
        return job;
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
