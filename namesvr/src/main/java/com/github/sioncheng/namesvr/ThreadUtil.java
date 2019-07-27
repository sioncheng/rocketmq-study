package com.github.sioncheng.namesvr;

public class ThreadUtil {

    public static void sleepQuiet(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {}
    }
}
