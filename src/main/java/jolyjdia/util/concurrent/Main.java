package jolyjdia.util.concurrent;

import jolyjdia.util.concurrent.cache.CacheBuilder;
import jolyjdia.util.concurrent.cache.ConcurrentCache;
import jolyjdia.util.concurrent.cache.FutureCache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.function.BiFunction;

public class Main {
    private static final int CANCEL = 1 << 5;
    private static final int SIGNAL = 1 << 4;
    private static final int CANCELLED = 1 << 3;
    private static final int REMOVING = 1 << 2;

    public static void main(String[] args) {
        FutureCache<Integer, String> cache = new CacheBuilder<Integer, String>()
                .removal(new CacheBuilder.RemovalListener<Integer, String>() {
                    @Override
                    public CompletableFuture<Boolean> onRemoval(Integer key, String v) {
                        return CompletableFuture.completedFuture(true);
                    }
                })
                .build0(new CacheBuilder.AsyncCacheLoader<Integer, String>() {
                    @Override
                    public CompletableFuture<String> asyncLoad(Integer key, Executor executor) {
                        return CompletableFuture.supplyAsync(() -> "dsadasda");
                    }
                });
        /*ConcurrentHashMap<Integer, CompletableFuture<String>> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 3; ++i) {
            new Thread(() -> {
                for (;;) {
                    int rand = ThreadLocalRandom.current().nextInt(3);
                    *//*map.compute(rand, new BiFunction<Integer, CompletableFuture<String>, CompletableFuture<String>>() {
                        @Override
                        public CompletableFuture<String> apply(Integer integer, CompletableFuture<String> stringCompletableFuture) {
                            if (stringCompletableFuture == null) {
                                return new CompletableFuture<>();
                            }
                            stringCompletableFuture.complete("хуй");
                            return stringCompletableFuture;
                        }
                    });*//*
                    cache.getAndPut(rand);
                }
            }).start();
            new Thread(() -> {
                for (; ; ) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    int rand = ThreadLocalRandom.current().nextInt(3);
                    cache.remove(rand);
                }
            }).start();
        }*/

        cache.put(1, CompletableFuture.completedFuture("CF 1")).thenRun(() -> {
            System.out.println(cache);
            cache.remove(1).thenRun(() -> {
                System.out.println(cache);
            });
            cache.put(1, CompletableFuture.supplyAsync(() -> {
                return "CF 2";
            })).thenRun(() -> {
                System.out.println(cache);
            });
        });
        for (;;) {}
    }
}
