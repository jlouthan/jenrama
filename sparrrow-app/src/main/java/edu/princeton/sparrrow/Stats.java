package edu.princeton.sparrrow;

import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;

public class Stats {
    private Dictionary<Integer, Integer> dict = new Hashtable<>();
    private String name;

    public Stats(String name) {
        this.name = name;
    }

    public void incrementCount(int id) {
        Integer val = dict.get(id);
        if (val == null) {
            dict.put(id, 1);
        } else {
            dict.put(id, val + 1);
        }
    }

    public int getCount(int id) {
        Integer val = dict.get(id);
        if (val == null) {
            return 0;
        } else {
            return val;
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Enumeration<Integer> e = dict.keys(); e.hasMoreElements();) {
           int id = e.nextElement();

           sb.append(id).append(":").append(dict.get(id)).append(" ");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

}
