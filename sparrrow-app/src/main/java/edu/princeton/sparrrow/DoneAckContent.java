package edu.princeton.sparrrow;

public class DoneAckContent extends MessageContent{
    private final int id;
    public DoneAckContent(int id){
        this.id = id;
    }
    public int getId(){ return id; }
}
