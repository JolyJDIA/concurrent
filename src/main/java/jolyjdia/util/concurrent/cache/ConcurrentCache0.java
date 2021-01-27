package jolyjdia.util.concurrent.cache;

import java.util.List;
import java.util.concurrent.*;

import java.io.Serial;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.stream.Collectors;

import static jolyjdia.util.concurrent.cache.ConcurrentCache.Node.SET_MASK;

/**
 * @param <K> - the type of keys
 * @param <V> - the type of values
 * @author JolyJDIA
 */
public class ConcurrentCache0<K,V> implements FutureCache<K,V>, Serializable {
    /*
     * The main purpose of this cache is completely asynchronous loading / unloading
     * guarantees fully consistent cache behavior
     * cache uses thread safe hash table
     * value - the node containing the write time and the last read
     * removing one of the "status" fields in ConcurrentCache is based on a non-blocking future
     * we cannot know when the deletion process will occur
     * cache safely sets up exactly one delete process for each sync block
     * to avoid double assignments that we might lose (otherwise it will be deleted two or more times)
     * deletion can only start after loading the value
     * double check locking is used for optimization
     * according to the semantics of the Java Memory Model, the field must be marked as volatile
     */

    /* ---------------- Constants -------------- */

    /** unstable nodes */
    static final int NESTED = -1;

    /** master planner for deleting expired Nodes */
    static final ScheduledExecutorService cleaner = Executors.newScheduledThreadPool(2, r -> {
        return AccessController.doPrivileged(new PrivilegedAction<>() {
            @Override
            public Thread run() {
                Thread thread = new Thread(r, "Cache-Cleaner");
                thread.setDaemon(true);
                thread.setPriority(Thread.MAX_PRIORITY - 2);
                return thread;
            }
        });
    });
    @Serial
    private static final long serialVersionUID = 6208003448940831310L;

    /* ---------------- Fields -------------- */
    private final ConcurrentHashMap<K,Node<K,V>> map;
    private final CacheBuilder.AsyncCacheLoader<? super K, V> cacheLoader;

    // builder
    private final Executor executor;
    private final CacheBuilder.RemovalListener<K,V> removalListener;

    // views
    private KeySetView<K,V> keySet;

    public ConcurrentCache0(CacheBuilder.AsyncCacheLoader<? super K, V> cacheLoader,
                           CacheBuilder<K, V> builder) {
        this.cacheLoader = cacheLoader;
        this.executor = builder.getExecutor();
        this.removalListener = builder.getRemoval();

        float loadFactor = builder.getLoadFactor();
        int buckets = (int)(builder.getMaxSize() / loadFactor) + 1;
        this.map = new ConcurrentHashMap<>(buckets, loadFactor);

        final long afterAccess = builder.getExpireAfterAccess(),
                tick = builder.getTick(),
                afterWrite = builder.getExpireAfterWrite();

        cleaner.scheduleAtFixedRate(() -> map.values().forEach(node -> {
            if (node.isRemoving()) {
                return;
            }
            long now = System.currentTimeMillis();
            if ((afterAccess != NESTED && now - node.lifetime >= afterAccess) ||
                (afterWrite  != NESTED && now - node.start    >= afterWrite)
            ) {
                CompletableFuture<Boolean> rem = node.withdrawal;
                if (rem != null && !rem.isCancelled()) {
                    return;
                }
                synchronized (node.f) {
                    if (node.withdrawal == null) {
                        node.withdrawal = node.safeRemoval();
                    }
                }
            }
        }), tick, tick, TimeUnit.MILLISECONDS);
    }
    /* ---------------- Nodes -------------- */

    private static class Node<K,V> {
        final ConcurrentCache0<K,V> cache;
        final K key;
        final CompletableFuture<V> cf;
        final long start = System.currentTimeMillis();
        volatile long lifetime = start;
        volatile CompletableFuture<Boolean> withdrawal;

        public Node(ConcurrentCache0<K,V> cache, K key) {
            this.cache = cache;
            this.key = key;
            this.cf = cache.cacheLoader.asyncLoad(key, cache.executor);
        }
        private final Object f = new Object();

        public CompletableFuture<Boolean> getWithdrawal() {
            CompletableFuture<Boolean> rem = withdrawal;
            if (rem != null && !rem.isCancelled()) {
                return rem;
            }
            synchronized (f) {
                if (withdrawal == null) {
                    withdrawal = safeRemoval();
                }
                return withdrawal;
            }
        }
        public boolean interruptRemoving() {
            lifetime = System.currentTimeMillis();
            CompletableFuture<Boolean> cf = withdrawal;
            if (cf != null && cf.isCancelled()) {
                return true;
            }
            synchronized (f) {
                cf = withdrawal;
                withdrawal = null;
            }
            return cf != null && !cf.isCancelled() && cf.cancel(true);
        }

        private CompletableFuture<Boolean> safeRemoval() {
            return cf.thenComposeAsync(f -> {
                return cache.removalListener.onRemoval(key, cf);
            }, cache.executor).thenApply(remove -> {
                if (remove) cache.map.remove(key);
                else signal();
                return remove;
            });
        }

        /**
         * Последовательно согласованы т к volatile
         * в связи с спецификацией JMM
         * если withdrawal = null, то есть гарантия, что lifetime актуален
         */
        private void signal() {
            lifetime = System.currentTimeMillis();
            withdrawal = null;
        }


        public final boolean isRemoving() {
            return withdrawal != null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node<?,?> node = (Node<?,?>) o;
            return start == node.start && cf.equals(node.cf);
        }

        @Override
        public int hashCode() {
            return cf.hashCode() ^ (int)(start ^ (start >>> 32));
        }
    }
    @Override
    public CompletableFuture<V> getAndPut(K key) {
        Node<K,V> node = map.get(key);
        if (node == null) {
            node = new Node<>(this, key);
        } else {
            node.interruptRemoving();
        }
        map.put(key, node);
        return node.cf;
        /*return map.compute(key, (k, oldValue) -> {
            if (oldValue == null) {
                return new Node<>(this, key);
            }
            oldValue.interruptRemoving();
            return oldValue;
        }).cf;*/
    }
    /* ---------------- Remove -------------- */

    @Override
    public CompletableFuture<Boolean> removeSafe(K key) {
        Node<K,V> node = map.get(key);
        if (node == null) {
            return CompletableFuture.failedFuture(new NullPointerException());
        }
        return node.getWithdrawal();
    }
    @Override
    public CompletableFuture<Boolean> remove(K key) {
        Node<K,V> node = map.remove(key);
        if (node == null) {
            return CompletableFuture.failedFuture(new NullPointerException());
        }
        return node.getWithdrawal();
    }
    @Override
    public List<CompletableFuture<Boolean>> removeAll() {
        return map.values().stream().map(Node::getWithdrawal).collect(Collectors.toList());
    }
    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public boolean isLoad(K key) {
        Node<K,V> node = map.get(key);
        return node != null && node.cf.isDone();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /* ---------------- Views -------------- */

    abstract static class CollectionView<K,V,E> {
        final ConcurrentCache<K,V> cache;//Поля файнл для безопасной гонки
        CollectionView(ConcurrentCache<K,V> cache) {
            this.cache = cache;
        }
        public final List<CompletableFuture<Boolean>> removeAll() {
            return cache.removeAll();
        }
        public final int size() {
            return cache.size();
        }
        public final boolean isEmpty() {
            return cache.isEmpty();
        }

        public final boolean isLoad(K key) {
            return cache.isLoad(key);
        }

        public abstract Iterator<E> iterator();
        public abstract boolean contains(K key);
        public abstract CompletableFuture<Boolean> remove(K key);
    }

    private static class KeySetView<K, V> extends CollectionView<K, V, K>
            implements Serializable {
        @Serial
        private static final long serialVersionUID = -3858239176353644889L;

        KeySetView(ConcurrentCache<K, V> cache) {
            super(cache);
        }

        @Override
        public Iterator<K> iterator() {
            return null;
        }

        @Override
        public boolean contains(K key) {
            return cache.containsKey(key);
        }

        @Override
        public CompletableFuture<Boolean> remove(K key) {
            return cache.remove(key);
        }

    }
    @Override
    public String toString() {
        return "ConcurrentCache{" + map + '}';
    }
}