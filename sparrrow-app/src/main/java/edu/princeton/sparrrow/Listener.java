package edu.princeton.sparrrow;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public abstract class Listener extends Thread {
    protected ObjectInputStream objInputStream;
    protected InputStream socketInputStream;
    protected boolean done = false;

    public void run() {

        if (objInputStream == null) {
            try {
                objInputStream = new ObjectInputStream(socketInputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        MessageContent m;

        try {
            while (!done || objInputStream.available() > 0) {
                m = ((Message) objInputStream.readObject()).getBody();
                handleMessage(m);
            }
            objInputStream.close();
        } catch(EOFException e){
            log("exiting because my parent is shutting down");
        } catch (IOException e) {
            if(e.getMessage() == "Stream closed."){
                log("exiting because the other end of my socket was closed");
            } else {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
    public abstract void handleMessage(MessageContent m);

    public void log(String m){
        System.out.println("Listener: " + m);
    }
}
