package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PipedInputStream;

public abstract class Listener extends Thread {
    protected ObjectInputStream inputStream;
    protected PipedInputStream pipeInputStream;

    public void run() {

        if (inputStream == null) {
            try {
                inputStream = new ObjectInputStream(pipeInputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        MessageContent m;

        while (true) {
            try {
                m = ((Message) inputStream.readObject()).getBody();
                handleMessage(m);

            } catch (IOException e) {
                e.printStackTrace();
                break;

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
    public abstract void handleMessage(MessageContent m);

}
