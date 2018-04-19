package edu.princeton.sparrrow;

import java.io.Serializable;

public class Message implements Serializable {

    private final MessageType type;
    private final MessageContent body;

    public Message(MessageType type, MessageContent body){
        this.type = type;
        this.body = body;
    }

    public MessageType getType() {
        return type;
    }
    public MessageContent getBody() {
        return body;
    }

}
