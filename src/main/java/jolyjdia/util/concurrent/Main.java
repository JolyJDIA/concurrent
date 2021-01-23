package jolyjdia.util.concurrent;

import jolyjdia.util.concurrent.cache.CacheBuilder;
import jolyjdia.util.concurrent.cache.ConcurrentCache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.*;

public class Main {
    public static void main0(String[] args) throws InterruptedException {


        Node node = new Node();
        node.set(new CompletableFuture<>());
        node.interruptRemoving();
        Thread.sleep(100);
        Executors.newCachedThreadPool()
        node.set(CompletableFuture.supplyAsync(() -> true));
        /**for (int i = 0; i < 5; ++i) {
            new Thread(() -> {
                for (;;) {
                    //node.interruptRemoving();
                }
            }).start();
            new Thread(() -> {
                for (;;) {
                    node.set(CompletableFuture.supplyAsync(() -> true));
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }*/
    }
    public static class Node {
        /**
         * статусы перехода
         * гарантирует атомарность операций используем CAS вместо sync block
         * из-за чрезвычайного уровня блокировок
        */
        private static final int NORMAL       = 0;
        private static final int DONE         = 1;
        private static final int COMPLETING   = 2;
        private static final int INTERRUPTING = 3;

        CompletableFuture<Boolean> rem;//можно опустить volatile т к я читаю один раз
        volatile int status = DONE;

        private CompletableFuture<Boolean> report() {
            if (status != INTERRUPTING) {
                return rem;
            }
            return null;
        }

        public boolean interruptRemoving() {
            if (!STATUS.compareAndSet(this, NORMAL, INTERRUPTING))
                return false;
            try {
                CompletableFuture<Boolean> cf = rem;
                if (cf != null && (!cf.isDone() && !cf.isCancelled())) {
                    cf.cancel(true);
                }
            } finally { // final state
                STATUS.setRelease(this, DONE);
            }
            return true;
        }
        protected void set(CompletableFuture<Boolean> v) {
            if (STATUS.compareAndSet(this, DONE, COMPLETING)) {
                System.out.println(" новая хрень");
                rem = v.thenApply(x -> {
                    STATUS.setRelease(this, DONE);
                    return x;
                });
                STATUS.setRelease(this, NORMAL);
            }
        }
        //todo:
        public boolean isCancelled() {
            return status == INTERRUPTING;
        }

        public boolean isDone() {
            return status == DONE;
        }

        // VarHandle mechanics
        private static final VarHandle STATUS;
        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                STATUS = l.findVarHandle(Node.class, "status", int.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    }

    public static void main(String[] args) {
        ConcurrentCache<Integer, String> cache = new CacheBuilder<Integer, String>()
                .removal(new CacheBuilder.RemovalListener<Integer, String>() {
                    @Override
                    public CompletableFuture<Boolean> onRemoval(Integer key, CompletableFuture<String> v) {
                        return v.thenApply(e -> {
                            return true;
                        });
                    }
                })
                .build(new CacheBuilder.AsyncCacheLoader<Integer, String>() {
                    @Override
                    public CompletableFuture<String> asyncLoad(Integer key, Executor executor) {
                        return CompletableFuture.supplyAsync(() -> "dsadasda");
                    }
                });
        cache.getAndPut(1).thenAccept(e -> {
           CompletableFuture.anyOf(cache.removeAll().toArray(new CompletableFuture[0])).thenRun(() -> {
               System.out.println(cache.size());
           });
        });
        for (;;) {}
    }
}
