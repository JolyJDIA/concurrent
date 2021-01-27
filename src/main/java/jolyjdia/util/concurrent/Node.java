package jolyjdia.util.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class Node {
    private final Object f = new Object();
    private volatile CompletableFuture<Boolean> withdrawal;
    private volatile long time;

    public CompletableFuture<Boolean> newWithdrawal() {
        CompletableFuture<Boolean> rem = withdrawal;
        if (rem != null && !rem.isCancelled()) {
            return rem;
        }
        synchronized (f) {
            if (withdrawal == null) {
                withdrawal = new CompletableFuture<Boolean>().thenApply(x -> {
                    signal();
                    return x;
                });
            }
            return withdrawal;
        }
    }

    private void signal() {
        time = 1;
        withdrawal = null;
    }

    public boolean interruptRemoving() {
        time = 1;
        CompletableFuture<Boolean> cf = withdrawal;
        if (cf != null && cf.isCancelled()) {//если успело отменить, то у меня уже null
            return true;
        }
        synchronized (f) {
            cf = withdrawal;
            withdrawal = null;
        }
        return cf != null && !cf.isCancelled() && cf.cancel(true);
    }
    public static void main(String[] args) {
        Node node = new Node();
        for (int i = 0; i < 3; ++i) {
            new Thread(() -> {
                for (;;) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //map.put(1, "dsdas");
                    node.newWithdrawal();
                }
            }).start();
            new Thread(() -> {
                for (;;) {
                    //map.remove(1);
                  //  if(node.interruptRemoving()) {
                       // System.out.println("БИНГО");
                    //}
                }
            }).start();
        }
    }
}
