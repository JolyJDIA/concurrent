package jolyjdia.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CompletableFuture;

public class Node {
    private final Object f = new Object();
    private volatile CompletableFuture<Boolean> unloadCf;
    private final CompletableFuture<Object> loaderCf = CompletableFuture.completedFuture("dasd");
    private volatile boolean signal;

    public CompletableFuture<Boolean> newWithdrawal() {
        CompletableFuture<Boolean> cfL = unloadCf;
        if ((cfL == null || cfL.isDone()) && signal) {
            return (CompletableFuture<Boolean>) UNLOAD.compareAndExchange(this, cfL,
            loaderCf.thenComposeAsync(f -> {
                return CompletableFuture.completedFuture(true);
            }).thenApply(remove -> {
                if (!remove || !signal) {
                    return false;
                }
                System.out.println("remove");
                return true;
            }).exceptionally(f -> false));
        }
        return unloadCf;
    }
    public CompletableFuture<Object> interruptRemoving() {
        signal = false;
        return (CompletableFuture<Object>) UNLOAD.compareAndExchange(this, unloadCf,
                unloadCf.thenApply(cancelled -> {
                    try {
                        if (!cancelled) {
                            System.out.println("put");
                        } return cancelled;
                    } finally {
                        signal = true;
                    }
                }));

    }
    public CompletableFuture<Object> put() {
        CompletableFuture<Object> cf = loaderCf;
        return (CompletableFuture<Object>) LOADER.compareAndExchange(this, cf,
                interruptRemoving().thenCompose(f -> {
                    return cf;
                }).thenCompose(x -> {
                    return CompletableFuture.completedFuture("newCf");
                }));
    }
    // VarHandle mechanics | Джава 8 ебана в жопу ее со своим AtomicReference
    private static final VarHandle LOADER, UNLOAD;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            LOADER = l.findVarHandle(Node.class, "loaderCf", CompletableFuture.class);
            UNLOAD = l.findVarHandle(Node.class, "unloadCf", CompletableFuture.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
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
