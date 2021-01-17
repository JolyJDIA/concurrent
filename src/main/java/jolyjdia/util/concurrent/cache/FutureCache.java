package jolyjdia.util.concurrent.cache;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface FutureCache<K,V> {

    CompletableFuture<V> getAndPut(K key);

    CompletableFuture<Boolean> removeSafe(K key);

    CompletableFuture<Boolean> remove(K key);

    List<CompletableFuture<Boolean>> removeAll();

    boolean containsKey(K key);

    boolean isLoad(K key);

    boolean isEmpty();

    int size();

    //entrySet

    //keySet
}