package jolyjdia.util.concurrent;

import jolyjdia.util.concurrent.cache.CacheBuilder;
import jolyjdia.util.concurrent.cache.ConcurrentCache;

import java.util.Map;
import java.util.concurrent.*;

public class Main {
    static volatile int i;
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
                        return CompletableFuture.completedFuture("dsadasda");
                    }
                });
        final int THREADS = 20;
        ConcurrentHashMap<Integer, CompletableFuture<String>> map = new ConcurrentHashMap<>();
        for (int i = 0; i < THREADS; ++i) {
            new Thread(() -> {
                for (;;) {
                    int rand = ThreadLocalRandom.current().nextInt(THREADS);
                    /*map.compute(rand, (k, old) -> {
                        if (old == null) {
                            return CompletableFuture.completedFuture("dsadasda");
                        }
                        old.thenRun(() -> {});
                        return old;
                    });*/
                    cache.getAndPut(rand);
                }
            }).start();
            new Thread(() -> {
                for (;;) {
                    int rand = ThreadLocalRandom.current().nextInt(THREADS);
                    /*CompletableFuture<String> cf = map.get(rand);
                    if (cf != null) {
                        map.remove(rand);
                    }*/
                    cache.removeSafe(rand);
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }
}
