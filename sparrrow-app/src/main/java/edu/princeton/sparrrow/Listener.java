package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.ObjectInputStream;

public abstract class Listener extends Thread {
    protected ObjectInputStream inputStream;

    public void run() {
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
