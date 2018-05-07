package edu.princeton.sparrrow;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public abstract class Listener extends Thread {
    protected ObjectInputStream objInputStream;
    protected InputStream socketInputStream;

    public void run() {

        if (objInputStream == null) {
            try {
                objInputStream = new ObjectInputStream(socketInputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        MessageContent m;

        while (true) {
            try {
                m = ((Message) objInputStream.readObject()).getBody();
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
