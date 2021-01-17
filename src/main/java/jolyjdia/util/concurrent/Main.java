package jolyjdia.util.concurrent;

import jolyjdia.util.concurrent.cache.JConcurrentHashMap;

import java.lang.invoke.VarHandle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Main {

    public static void main(String[] args) {
        JConcurrentHashMap<Integer, String> map = new JConcurrentHashMap<>();
        ConcurrentSkipListMap
        new Thread(() -> {
            for (int i = 0; i < 20; ++i) {
                map.put(i, CompletableFuture.supplyAsync(() -> "dasd"), false);
            }
        }).start();
        for (int i = 20; i < 40; ++i) {
            map.put(i, CompletableFuture.supplyAsync(() -> "dasd"), false);
        }
    }
}
