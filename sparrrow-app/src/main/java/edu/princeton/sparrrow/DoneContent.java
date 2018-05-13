package edu.princeton.sparrrow;

public class DoneContent extends MessageContent {
    private final int id;
    public DoneContent(int id){
        this.id = id;
    }
    public int getId(){ return id; }
}
