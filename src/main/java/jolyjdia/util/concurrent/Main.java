package jolyjdia.util.concurrent;

import jolyjdia.util.concurrent.cache.CacheBuilder;
import jolyjdia.util.concurrent.cache.ConcurrentCache;
import jolyjdia.util.concurrent.cache.FutureCache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.*;

public class Main {

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
        cache.put(1, CompletableFuture.completedFuture("CF 1")).thenRun(() -> {
            System.out.println(cache);
            cache.remove(1);
            cache.put(1, CompletableFuture.supplyAsync(() -> {
                return "CF 2";
            })).thenRun(() -> {
                System.out.println(cache);
            });
        });
        for (;;) {}
    }
}
