package jolyjdia.util.concurrent.cache;

import java.util.concurrent.*;

import java.io.Serial;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.function.Function;

/**
 * @param <K> - the type of keys
 * @param <V> - the type of values
 * @author JolyJDIA
 */
public class ConcurrentCache0<K,V> {
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

    /* ---------------- Fields -------------- */
    private final JConcurrentHashMap<K,V> map;
    private final CacheBuilder.AsyncCacheLoader<? super K, V> cacheLoader;

    // builder
    private final Executor executor;
    private final CacheBuilder.RemovalListener<K,V> removalListener;


    public ConcurrentCache0(CacheBuilder.AsyncCacheLoader<? super K, V> cacheLoader,
                            CacheBuilder<K, V> builder) {
        this.cacheLoader = cacheLoader;
        this.executor = builder.getExecutor();
        this.removalListener = builder.getRemoval();

        //this.map = new ConcurrentHashMap<>(buckets, loadFactor);
        this.map = new JConcurrentHashMap<>();
        final long afterAccess = builder.getExpireAfterAccess(),
                tick = builder.getTick(),
                afterWrite = builder.getExpireAfterWrite();
        cleaner.scheduleAtFixedRate(() -> map.forEach((node) -> {
            //последовательно согласованы
            if (node.removal != null) {//non sync - не важно
                return;
            }
            long now = System.currentTimeMillis();
            if ((afterAccess != NESTED && now - node.refresh >= afterAccess) ||
                (afterWrite  != NESTED && now - node.start   >= afterWrite)
            ) {
                CompletableFuture<V> cf = node.val; K key = node.key;
                map.removeAfter(node, () -> cf.thenComposeAsync(f -> {
                    return removalListener.onRemoval(key, cf);
                }, executor));
            }
        }), tick, tick, TimeUnit.MILLISECONDS);
    }
    /*
       +-----------------------+-----------------------+-----------------------+
       |        Thread1        |        Thread2        |        Thread3        |
       | sync() {              |     (not correct)     | sync() {              |
       | if (rem == null) true |   rem == null = true  |   rem == null = false |
       |      rem = newRem     |                       | }                     |
       +-----------------------+-----------------------+-----------------------+
    */

    public CompletableFuture<V> getAndPut(K key) {
        return map.compute(key, (k, oldValue) -> {
            if (oldValue == null) {
                return cacheLoader.asyncLoad(key, executor);
            }
            return oldValue;
        });
    }
    public CompletableFuture<Boolean> removeAfter(K key) {
        return map.removeAfter(key, new Function<JConcurrentHashMap.Node<K, V>, CompletableFuture<Boolean>>() {
            @Override
            public CompletableFuture<Boolean> apply(JConcurrentHashMap.Node<K, V> kvNode) {
                if (kvNode == null) {

                }
                CompletableFuture<V> cf = kvNode.val;
                return cf.thenComposeAsync(f -> {
                    return removalListener.onRemoval(key, cf);
                });
            }
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

    public int size() {
        return map.size();
    }

    private CompletableFuture<Boolean> safeRemoval(K key, JConcurrentHashMap.Node<K, V> node) {
        CompletableFuture<V> cf = node.val;
        return cf.thenComposeAsync(f -> {
            return removalListener.onRemoval(key, cf);
        }, executor).thenApply(remove -> {
            if (remove)
                map.replaceNode(key, null, null);
            else synchronized (node) {
                node.removal = null;
                node.refresh = System.currentTimeMillis();
            }
            return remove;
        });
    }
    @Override
    public String toString() {
        return "ConcurrentCache{" + map + '}';
    }
}
