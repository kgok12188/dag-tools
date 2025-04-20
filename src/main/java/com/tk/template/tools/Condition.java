package com.tk.template.tools;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;



interface Condition {

    long PARK_TIMEOUT = 50L;

    int MAX_PROG_YIELD = 2000;

    boolean test();


    void awaitNanos(final long timeout) throws InterruptedException;

    void await() throws InterruptedException;

    void signal();

    static int progressiveYield(final int n) {
        if(n > 500) {
            if(n<1000) {
                // "randomly" yield 1:8
                if((n & 0x7) == 0) {
                    LockSupport.parkNanos(PARK_TIMEOUT);
                } else {
                    onSpinWait();
                }
            } else if(n<MAX_PROG_YIELD) {
                // "randomly" yield 1:4
                if((n & 0x3) == 0) {
                    Thread.yield();
                } else {
                    onSpinWait();
                }
            } else {
                Thread.yield();
                return n;
            }
        } else {
            onSpinWait();
        }
        return n+1;
    }

    static void onSpinWait() {
        // Java 9 hint for spin waiting PAUSE instruction
        //http://openjdk.java.net/jeps/285
        // Thread.onSpinWait();
    }

    static boolean waitStatus(final long timeout, final TimeUnit unit, final Condition condition) throws InterruptedException {
        // until condition is signaled
        final long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);
        final long expireTime = System.nanoTime() + timeoutNanos;
        // the queue is empty or full wait for something to change
        while (condition.test()) {
            final long now = System.nanoTime();
            if (now > expireTime) {
                return false;
            }
            condition.awaitNanos(expireTime - now - PARK_TIMEOUT);
        }
        return true;
    }

}