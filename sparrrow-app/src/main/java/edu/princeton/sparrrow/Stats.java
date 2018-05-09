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

    public Enumeration<Integer> getKeys() {
        return dict.keys();
    }

    public String keysToString(Enumeration<Integer> e) {
        StringBuilder sb = new StringBuilder();
        String separator = " ";

        while (e.hasMoreElements()) {
            int id = e.nextElement();

            sb.append(id).append(separator);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public String toString(Enumeration<Integer> e) {
        StringBuilder sb = new StringBuilder();
        String separator = " ";

        while (e.hasMoreElements()) {
            int id = e.nextElement();

            sb.append(getCount(id)).append(separator);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        String separator = " ";

        for (Enumeration<Integer> e = dict.keys(); e.hasMoreElements();) {
           int id = e.nextElement();

           sb.append(id).append(":").append(getCount(id)).append(separator);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    // Creates a String of lines of the stats for the ids of s1, of the form
    // " [<id>] <s1count> <s2count>"
    public static String stringStats(Stats s1, Stats s2) {
        StringBuilder sb = new StringBuilder();

        String preface = " ";
        String separator = " ";

        for (Enumeration<Integer> e = s1.getKeys(); e.hasMoreElements();) {
            int id = e.nextElement();

            sb.append(preface);
            sb.append("[").append(id).append("]").append(separator);
            sb.append(s1.getCount(id)).append(separator);
            sb.append(s2.getCount(id));
            sb.append("\n");
        }

        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

}
