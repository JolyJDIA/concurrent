package jolyjdia.util.concurrent.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import java.io.Serial;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.function.Supplier;

import static jolyjdia.util.concurrent.cache.ConcurrentCache.Node.*;

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
            if ((node.status & (SET | SIGNAL | COMPLETING | INTERRUPTING)) != 0) {//если готов или инициализирован
                return;
            }
            long now = System.currentTimeMillis();
            if ((afterAccess != NESTED && now - node.refresh >= afterAccess) ||
                (afterWrite  != NESTED && now - node.start   >= afterWrite)
            ) {
                node.set(() -> safeRemoval(key, node));
            }
        }), tick, tick, TimeUnit.MILLISECONDS);
    }
    /* ---------------- Nodes -------------- */

    static class Node<V> {
        /**
         * статусы перехода
         * гарантирует атомарность операций используем CAS вместо sync block
         * из-за чрезвычайного уровня блокировок
         */
        static final int SET          = 1 << 31; // 1 must be negative
        static final int COMPLETING   = 1 << 19; // 2
        static final int CANCELLED    = 1 << 18; // 3
        static final int INTERRUPTING = 1 << 17; // 4
        static final int SIGNAL       = 1 << 16; // 5 true if rem is done

        final CompletableFuture<V> cf;
        final long start = System.currentTimeMillis();
        CompletableFuture<Boolean> rem; // можно опустить volatile т.к. я читаю один раз
        volatile long refresh = start;
        volatile int status = SIGNAL;

        public Node(CompletableFuture<V> cf) {
            this.cf = cf;
        }

        public CompletableFuture<Boolean> getRemoval() {
            int s = status;
            CompletableFuture<Boolean> r = rem;//До
            if (s == SET)
                return r;
            else if ((s & (CANCELLED | INTERRUPTING)) != 0) {
               return CompletableFuture.completedFuture(false);
            }
            return null;
        }

        boolean interruptRemoving() {
            for(int s;;) {
                //set or completing
                if ((s = status) > COMPLETING)
                    return false;
                else if (STATUS.weakCompareAndSet(this, s, INTERRUPTING)) {
                    try {
                        refresh = System.currentTimeMillis();
                        CompletableFuture<Boolean> cf = rem;
                        if (cf != null && (!cf.isDone() && !cf.isCancelled())) {
                            cf.cancel(true);
                        }
                    } finally { // final state
                        STATUS.setRelease(this, CANCELLED);
                    }
                    return true;
                }
            }
        }
        private static final int SET_MASK = (SIGNAL | CANCELLED);

        /*
         +============+=============+
         | COMPLETING |  SET(старое)|
         |   set      |  get        |
         |            |             |
         +=============+=============+
         */
        private CompletableFuture<Boolean> set(Supplier<CompletableFuture<Boolean>> supplier) {
            int s = status;
            CompletableFuture<Boolean> r = rem;
            if (r != null && r.isCancelled()) {
                r = CompletableFuture.completedFuture(false);
            }
            if (s == SET)
                return r;
            else if (s == INTERRUPTING)
                return CompletableFuture.completedFuture(false);
            else if (STATUS.compareAndSet(this, (s & SET_MASK), COMPLETING)) {//done or init
                try {
                    return rem = supplier.get().thenApply(x -> {
                        STATUS.setRelease(this, SIGNAL);
                        return x;
                    });
                } finally {
                    STATUS.setRelease(this, SET);
                }
            } else if (r == null)
                r = CompletableFuture.completedFuture(false);
            return r;
        }

        public final boolean isDone() {
            return status == SIGNAL;
        }
        public final boolean isCancelled() {
            return (status & (INTERRUPTING | CANCELLED)) != 0;
        }
        public final boolean isRemoval() {
            return (status & (SET | SIGNAL | COMPLETING | INTERRUPTING)) != 0;
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
                return new Node<>(cacheLoader.asyncLoad(key, executor));
            }
            oldValue.interruptRemoving();
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
            else
                node.refresh = System.currentTimeMillis();
            return remove;
        });
    }
    /* ---------------- Remove -------------- */

    @Override
    public CompletableFuture<Boolean> removeSafe(K key) {
        Node<V> node = map.get(key);
        if (node == null) {
            return CompletableFuture.failedFuture(new NullPointerException());
        }
        return node.set(() -> safeRemoval(key, node));
    }
    @Override
    public CompletableFuture<Boolean> remove(K key) {
        Node<V> node = map.remove(key);
        if (node == null) {
            return CompletableFuture.failedFuture(new NullPointerException());
        }
        CompletableFuture<Boolean> rem;
        if ((rem = node.getRemoval()) == null) {
            rem = node.set(() -> safeRemoval(key, node));
        }
        return rem;
    }
    @Override
    public List<CompletableFuture<Boolean>> removeAll() {
        List<CompletableFuture<Boolean>> cfs = new LinkedList<>();

        map.forEach((key, node) -> {
            CompletableFuture<Boolean> removal;
            if ((removal = node.getRemoval()) == null) {
                removal = node.set(() -> safeRemoval(key, node));
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