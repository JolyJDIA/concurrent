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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private transient KeySetView<K,V> keySet;
    private transient ValuesView<K,V> values;

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
            if (afterAccess != NESTED && now - node.lifetime >= afterAccess ||
                afterWrite  != NESTED && now - node.start    >= afterWrite
            ) { node.newWithdrawal(); }
        }), tick, tick, TimeUnit.MILLISECONDS);
    }
    /* ---------------- Nodes -------------- */

    private static class Node<K,V> {
        final ConcurrentCache0<K,V> cache;
        final K key;
        final long start = System.currentTimeMillis();
        volatile long lifetime = start;
        volatile CompletableFuture<Boolean> unloadCf;
        volatile CompletableFuture<V> loaderCf;
        private volatile boolean signal = true;

        public Node(ConcurrentCache0<K, V> cache, K key, CompletableFuture<V> loader) {
            this.cache = cache;
            this.key = key;
            this.loaderCf = loader;
        }

        public CompletableFuture<Boolean> newWithdrawal() {
            CompletableFuture<Boolean> cfL = unloadCf;
            if ((cfL == null || cfL.isDone()) && signal) {
                return (CompletableFuture<Boolean>) UNLOAD.compareAndExchange(this, cfL,
                        loaderCf.thenComposeAsync(f -> {
                            return cache.removalListener.onRemoval(key, f);
                        }, cache.executor).thenApply(remove -> {
                            boolean cancel = !(remove && signal);
                            if (!cancel) {
                                cache.map.remove(key);
                            }
                            lifetime = System.currentTimeMillis();
                            return cancel;
                        }).exceptionally(f -> false));
            }
            return cfL;
        }
        public CompletableFuture<V> interruptRemoving() {
            signal = false;
            CompletableFuture<V> lCf = loaderCf; CompletableFuture<Boolean> r = unloadCf;
            if (r == null) return lCf;
            return ((CompletableFuture<Boolean>) UNLOAD.compareAndExchange(this, lCf,
                    r.thenApply(cancelled -> {
                        try {
                            if (!cancelled) {
                                cache.map.put(key, this);
                            } return cancelled;
                        } finally {
                            signal = true;
                        }
                    }))
            ).thenCompose(f -> lCf);
        }
        public CompletableFuture<V> put(CompletableFuture<V> newCf) {
            CompletableFuture<V> cf = loaderCf; CompletableFuture<Boolean> r = unloadCf;
            return (CompletableFuture<V>) LOADER.compareAndExchange(this, cf,
                    (r == null ? newCf : (r.thenAcceptBoth(cf, (cancelled, v) -> {
                        try {
                            if (!cancelled) {
                                cache.map.put(key, this);
                            }
                        } finally {
                            signal = true;
                        }
                    }).thenCompose(x -> newCf))));
        }

        public final boolean isRemoving() {
            CompletableFuture<Boolean> uCf = unloadCf;
            return uCf != null && !uCf.isDone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node<?,?> node = (Node<?,?>) o;
            return key.equals(node.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
        @Override
        public String toString() {
            CompletableFuture<V> cf = loaderCf;
            String string = cf.toString();
            if (cf.isDone()) {
                string = " result=" + cf.join();
            }
            return string;
        }
        // VarHandle mechanics
        private static final VarHandle LOADER, UNLOAD;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                LOADER = l.findVarHandle(Node.class, "loaderCf", CompletableFuture.class);
                UNLOAD = l.findVarHandle(Node.class, "unloadCf", CompletableFuture.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    }
    @Override
    public CompletableFuture<V> getAndPut(K key) {
        CompletableFuture<V> start = new CompletableFuture<>();
        //Не compute т к interruptRemoving может прямо сейчас делать структурные модификации мапы
        //что нарушает спецификацию
        Node<K,V> node = map.putIfAbsent(key, new Node<>(this, key, start));
        if (node == null) {
            cacheLoader.asyncLoad(key, executor).thenAccept(x -> {
                System.out.println("----------- new load -----------");
                start.complete(x);
            });
            return start;
        } else {
            return node.interruptRemoving();
        }
    }
    @Override
    public CompletableFuture<V> put(K key, CompletableFuture<V> cf) {
        Node<K,V> node = map.putIfAbsent(key, new Node<>(this, key, cf));
        if (node == null) {
            cf.thenAccept(x -> {
                System.out.println("----------- new load -----------");
            });
            return cf;
        }
        return node.put(cf);
    }

    @Override
    public CompletableFuture<Boolean> removeSafe(K key) {
        Node<K, V> node = map.get(key);//есть
        //уже удалил
        //remove 2x
        return node == null ? CompletableFuture.failedFuture(new NullPointerException()) : node.newWithdrawal();
    }
    @Override
    public CompletableFuture<Boolean> remove(K key) {
        return removeSafe(key);
    }
    @Override
    public List<CompletableFuture<Boolean>> removeAll() {
        return this.map.values().stream().map(n -> {
            return n.newWithdrawal();
        }).collect(Collectors.toList());
    }
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public boolean isLoad(K key) {
        Node<K,V> node = map.get(key);
        return node != null && node.loaderCf.isDone();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    //  Безопасные гонки
    public KeySetView<K,V> keySet() {
        KeySetView<K,V> ks;
        if ((ks = keySet) != null) return ks;
        return keySet = new KeySetView<>(this);
    }
    public ValuesView<K, V> values() {
        ValuesView<K,V> vs;
        if ((vs = values) != null) return vs;
        return values = new ValuesView<>(this);
    }

    /* ---------------- Views -------------- */

    static abstract class CollectionView<K,V, E> {
        final ConcurrentCache0<K,V> cache;//Поля файнл для безопасной гонки
        CollectionView(ConcurrentCache0<K,V> cache) {
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
        public boolean contains(K key) {
            return cache.containsKey(key);
        }
        public CompletableFuture<Boolean> remove(K key) {
            return cache.remove(key);
        }
        public abstract IteratorCache<E> iterator();
    }

    private static class KeySetView<K, V> extends CollectionView<K, V, K>
            implements Serializable {
        @Serial
        private static final long serialVersionUID = -3858239176353644889L;

        KeySetView(ConcurrentCache0<K, V> cache) {
            super(cache);
        }
        @Override
        public KeyIterator<K, V> iterator() {
            return new KeyIterator<>(cache);
        }
    }
    static final class ValuesView<K,V> extends CollectionView<K,V, CompletableFuture<V>>
            implements java.io.Serializable {
        @Serial
        private static final long serialVersionUID = 2249069246763182397L;
        ValuesView(ConcurrentCache0<K,V> cache) { super(cache); }

        @Override
        public final ValueIterator<K,V> iterator() {
            return new ValueIterator<>(cache);
        }
    }

    static final class KeyIterator<K,V> implements IteratorCache<K> {
        final ConcurrentCache0<K,V> cache;
        final Iterator<Map.Entry<K, Node<K,V>>> iterator;
        K last;
        KeyIterator(ConcurrentCache0<K,V> cache) {
            this.cache = cache;
            this.iterator = cache.map.entrySet().iterator();
        }
        public final boolean hasNext() {
            return iterator.hasNext();
        }
        public final K next() {
            return (last = iterator.next().getKey());
        }
        public final CompletableFuture<Boolean> remove() {
            K k;
            if ((k = last) == null)
                throw new IllegalStateException();
            last = null;
            return cache.remove(k);
        }
    }
    static final class ValueIterator<K,V> implements IteratorCache<CompletableFuture<V>> {
        final ConcurrentCache0<K,V> cache;
        final Iterator<Map.Entry<K, Node<K,V>>> iterator;
        Node<K,V> last;
        ValueIterator(ConcurrentCache0<K,V> cache) {
            this.cache = cache;
            this.iterator = cache.map.entrySet().iterator();
        }
        public final boolean hasNext() {
            return iterator.hasNext();
        }
        public final CompletableFuture<V> next() {
            return (last = iterator.next().getValue()).loaderCf;
        }
        public final CompletableFuture<Boolean> remove() {
            Node<K,V> p;
            if ((p = last) == null)
                throw new IllegalStateException();
            last = null;
            return cache.remove(p.key);
        }
    }
    public interface IteratorCache<E> {
        boolean hasNext();

        E next();

        default CompletableFuture<Boolean> remove() {
            throw new UnsupportedOperationException("remove");
        }
        default void forEachRemaining(Consumer<? super E> action) {
            Objects.requireNonNull(action);
            while (hasNext())
                action.accept(next());
        }
    }


    @Override
    public String toString() {
        return "ConcurrentCache{" + map + '}';
    }
}