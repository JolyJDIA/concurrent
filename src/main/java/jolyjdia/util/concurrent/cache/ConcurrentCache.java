package jolyjdia.util.concurrent.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.*;

import java.io.Serial;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static jolyjdia.util.concurrent.cache.ConcurrentCache.Node.SET_MASK;

/**
 * @param <K> - the type of keys
 * @param <V> - the type of values
 * @author JolyJDIA
 */
public class ConcurrentCache<K,V> implements FutureCache<K,V>, Serializable {
    /* ---------------- Constants -------------- */

    /** unstable nodes */
    static final int NESTED = -1;

    /** master planner for deleting expired Nodes */
    static final ScheduledExecutorService cleaner = Executors.newScheduledThreadPool(1, r -> {
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
            if ((node.status & SET_MASK) == 0) {
                return;
            }
            long now = System.currentTimeMillis();
            if ((afterAccess != NESTED && now - node.refresh >= afterAccess) ||
                    (afterWrite  != NESTED && now - node.start   >= afterWrite)
            ) {
                node.set0(key);
            }
        }), tick, tick, TimeUnit.MILLISECONDS);
    }
    /* ---------------- Nodes -------------- */

    static class Node<K, V> {
        /**
         * статусы перехода
         * гарантирует атомарность операций используем CAS вместо sync block
         * из-за чрезвычайного уровня блокировок
         */
        static final int SET          = 1 << 31; // 1 must be negative
        static final int COMPLETING   = 1 << 19; // 2
        static final int SIGNAL       = 1 << 18; // 3 true if rem is done
        static final int INTERRUPTING = 1 << 17; // 4
        static final int CANCELLED    = 1 << 16; // 5

        final CompletableFuture<V> cf;
        final long start = System.currentTimeMillis();
        final ConcurrentCache<K, V> cache;
        CompletableFuture<Boolean> rem; // можно опустить volatile т.к. я читаю один раз
        long refresh = start;
        volatile int status = SIGNAL;

        public Node(ConcurrentCache<K, V> cache, CompletableFuture<V> cf) {
            this.cache = cache;
            this.cf = cf;
        }

        private CompletableFuture<Boolean> awaitGet(int s) {
            for (;;) {
                if (s == COMPLETING) {
                    s = status;
                } else if (s >= INTERRUPTING) {
                    return CompletableFuture.completedFuture(false);
                } else {
                    return rem;
                }
            }
        }
        @SuppressWarnings("uncheked")
        boolean interruptRemoving() {
            refresh = System.currentTimeMillis();
            for(int s;;) {
                if ((s = status) == SIGNAL)//поздно пить боржоми, когда почки отказали
                    return false;
                else if(s == CANCELLED)
                    return true;
                    //если не сет то COMPLETING или INTERRUPTING оба ждем...
                else if (s == SET && STATUS.weakCompareAndSet(this, SET, INTERRUPTING)) {
                    try {
                        CompletableFuture<Boolean> future = rem;
                        return future != null && (!future.isDone() && !future.isCancelled()) && future.cancel(true);
                    } finally { // final state
                        STATUS.setRelease(this, CANCELLED);
                    }
                }
            }
        }
        static final int SET_MASK = (SIGNAL | CANCELLED);

        private CompletableFuture<Boolean> set(K key) {
            int s;
            if (STATUS.compareAndSet(this, ((s = status) & SET_MASK), COMPLETING)) {//done or init
                try {
                    return rem = cf.thenComposeAsync(f -> {
                        return cache.removalListener.onRemoval(key, f);
                    }, cache.executor).thenApply(remove -> {
                        if (remove) {
                            cache.map.remove(key);
                        } else {
                            refresh = System.currentTimeMillis();
                        }
                        STATUS.setRelease(this, SIGNAL);
                        return remove;
                    });
                } finally {
                    STATUS.setRelease(this, SET);
                }
            }
            return awaitGet(s);
        }
        private void set0(K key) {
            if (STATUS.compareAndSet(this, (status & SET_MASK), COMPLETING)) {//done or init
                try {
                    rem = cf.thenComposeAsync(f -> {
                        return cache.removalListener.onRemoval(key, f);
                    }, cache.executor).thenApply(remove -> {
                        if (remove) {
                            cache.map.remove(key);
                        } else {
                            refresh = System.currentTimeMillis();
                        }
                        STATUS.setRelease(this, SIGNAL);
                        return remove;
                    });
                } finally {
                    STATUS.setRelease(this, SET);
                }
            }
        }

        public final boolean isDone() {
            return status == SIGNAL;
        }

        public final boolean isCancelled() {
            return (status & (INTERRUPTING | CANCELLED)) != 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node<?, ?> node = (Node<?, ?>) o;
            return start == node.start && cf.equals(node.cf);
        }

        @Override
        public int hashCode() {
            return cf.hashCode() ^ (int)(start ^ (start >>> 32));
        }
        // VarHandle mechanics | Джава 8 ебана в жопу ее со своим AtomicReference
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
    @Override
    public CompletableFuture<V> getAndPut(K key) {
        return map.compute(key, (k, oldValue) -> {
            if (oldValue == null) {
                return new Node<>(this, cacheLoader.asyncLoad(key, executor));
            }
            if(oldValue.interruptRemoving()) {

            }
            return oldValue;
        }).cf;
    }

    @Override
    public CompletableFuture<V> put(K key, CompletableFuture<V> v) {
        return null;
    }

    /* ---------------- Remove -------------- */

    @Override
    public CompletableFuture<Boolean> removeSafe(K key) {
        Node<K, V> node = map.get(key);
        if (node == null) {
            return CompletableFuture.failedFuture(new NullPointerException());
        }
        return node.set(key);
    }
    @Override
    public CompletableFuture<Boolean> remove(K key) {
        return removeSafe(key);
    }
    @Override
    public List<CompletableFuture<Boolean>> removeAll() {
        return map.entrySet().stream().map(x -> {
            Node<K, V> node = x.getValue();
            return node.set(x.getKey());
        }).collect(Collectors.toList());
    }
    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public boolean isLoad(K key) {
        Node<K, V> node = map.get(key);
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