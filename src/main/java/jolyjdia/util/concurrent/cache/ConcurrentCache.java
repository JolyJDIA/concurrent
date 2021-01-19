package jolyjdia.util.concurrent.cache;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import java.io.Serial;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @param <K> - the type of keys
 * @param <V> - the type of values
 * @author JolyJDIA
 */
public class ConcurrentCache<K,V> implements FutureCache<K,V>, Serializable {
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
    private final ConcurrentHashMap<K,Node<V>> map;
    private final CacheBuilder.AsyncCacheLoader<? super K, V> cacheLoader;

    // builder
    private final Executor executor;
    private final CacheBuilder.RemovalListener<K,V> removalListener;

    // views
    private KeySetView<K,V> keySet;

    public ConcurrentCache(CacheBuilder.AsyncCacheLoader<? super K, V> cacheLoader,
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

        cleaner.scheduleAtFixedRate(() -> map.forEach((key, node) -> {
            //последовательно согласованы
            if (node.isRemoving()) {//non sync - не важно
                return;
            }
            //rem = null | refresh = latest
            long now = System.currentTimeMillis();
            if ((afterAccess != NESTED && now - node.refresh >= afterAccess) ||
                (afterWrite  != NESTED && now - node.start   >= afterWrite)
            ) {
                synchronized (node.removalLock) {
                    CompletableFuture<Boolean> rem = node.removal;
                    if (rem == null || rem != Node.INTERRUPT_REMOVAL) {//гарантированно что refresh последний
                        if (now - node.refresh < afterAccess)
                            return;
                        node.removal = safeRemoval(key, node);
                    }
                }
            }
        }), tick, tick, TimeUnit.MILLISECONDS);
    }
    /* ---------------- Nodes -------------- */

    private static class Node<V> {
        //3-тье состояние
        static final CompletableFuture<Boolean> INTERRUPT_REMOVAL = CompletableFuture.failedFuture(
                new UnsupportedOperationException()//todo: hashCode
        );
        final CompletableFuture<V> cf;
        final long start = System.currentTimeMillis();
        volatile long refresh = start;
        volatile CompletableFuture<Boolean> removal;
        private final Object removalLock = new Object();

        public Node(CompletableFuture<V> cf) {
            this.cf = cf;
        }
        public final boolean isRemoving() {
            return removal != null && removal != INTERRUPT_REMOVAL;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node<?> node = (Node<?>) o;
            return start == node.start && cf.equals(node.cf);
        }

        @Override
        public int hashCode() {
            return cf.hashCode() ^ (int)(start ^ (start >>> 32));
        }
    }
    /*
       +-----------------------+-----------------------+-----------------------+
       |        Thread1        |        Thread2        |        Thread3        |
       |                       | sync() {              | sync() {              |
       |       rem = null      | if (rem == null) true | if (rem == null) false|
       |                       |      rem = newRem     |      rem = newRem     |
       +-----------------------+-----------------------+-----------------------+
    */
    @Override
    public CompletableFuture<V> getAndPut(K key) {
        return map.compute(key, (k, oldValue) -> {
            if (oldValue == null) {
                return new Node<>(cacheLoader.asyncLoad(key, executor));
            }
            final long now = System.currentTimeMillis();

            CompletableFuture<Boolean> rem = oldValue.removal;
            if (rem == Node.INTERRUPT_REMOVAL || rem == null) {
                synchronized (oldValue.removalLock) { rem = oldValue.removal; }
            }
            oldValue.refresh = now;
            oldValue.removal = Node.INTERRUPT_REMOVAL;
            if (rem != Node.INTERRUPT_REMOVAL && (rem != null && !rem.isDone())) {
                rem.cancel(true);
            }
            return oldValue;
        }).cf;
    }
    private CompletableFuture<Boolean> safeRemoval(K key, Node<V> node) {
        CompletableFuture<V> cf = node.cf;
        return cf.thenComposeAsync(f -> {
            return removalListener.onRemoval(key, cf);
        }, executor).thenApply(remove -> {
            if (remove)
                map.remove(key);
            else {
                //последовательно согласованы
                node.refresh = System.currentTimeMillis();
                node.removal = null;
            }
            return remove;
        });
    }
    /* ---------------- Remove -------------- */
    /*
       +-----------------------+-----------------------+-----------------------+
       |        Thread1        |        Thread2        |        Thread3        |
       |                       | sync() {              | sync() {              |
       |       rem = null      | if (rem == null) true | if (rem == null) false|
       |                       |      rem = newRem     |      rem = newRem     |
       +-----------------------+-----------------------+-----------------------+
    */

    @Override
    public CompletableFuture<Boolean> removeSafe(K key) {
        Node<V> node = map.get(key);
        if (node == null) {
            return CompletableFuture.failedFuture(new NullPointerException());
        }
        CompletableFuture<Boolean> rem = node.removal;
        if (rem != null && rem != Node.INTERRUPT_REMOVAL) {
            return rem;
        }
        synchronized (node.removalLock) {
            CompletableFuture<Boolean> cf = node.removal;//!= null
            if (cf == null || cf != Node.INTERRUPT_REMOVAL) {
                node.removal = safeRemoval(key, node);
            }
            return cf;
        }
    }
    @Override
    public CompletableFuture<Boolean> remove(K key) {
        Node<V> node = map.remove(key);
        if (node == null) {
            return CompletableFuture.failedFuture(new NullPointerException());
        }
        CompletableFuture<Boolean> rem = node.removal;
        if (rem != null) {
            return rem;
        }
        synchronized (node.removalLock) {
            CompletableFuture<Boolean> cf = node.removal;
            if (cf == null) {
                CompletableFuture<V> cf0 = node.cf;
                return node.removal = cf0.thenComposeAsync(f -> {
                    return removalListener.onRemoval(key, cf0);
                }, executor);
            }
            return cf;
        }
    }
    @Override
    public List<CompletableFuture<Boolean>> removeAll() {
        List<CompletableFuture<Boolean>> cfs = new LinkedList<>();

        map.forEach((key, node) -> {
            CompletableFuture<Boolean> removal;

            if ((removal = node.removal) == null) {
                synchronized (node.removalLock) {
                    CompletableFuture<Boolean> cf = node.removal;
                    removal = cf == null ? (node.removal = safeRemoval(key, node)) : cf;
                }
            }
            cfs.add(removal);
        });
        return cfs;
    }
    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public boolean isLoad(K key) {
        Node<V> node = map.get(key);
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

    //Безопасная гонка
    public KeySetView<K,V> keySet() {
        KeySetView<K,V> ks;
        if ((ks = keySet) != null) return ks;
        return keySet = new KeySetView<>(this);
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