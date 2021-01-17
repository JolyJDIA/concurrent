package jolyjdia.util.concurrent.cache;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class CacheBuilder<K, V> {
    private Executor executor = ForkJoinPool.commonPool();
    private long expireAfterAccess = -1, expireAfterWrite = -1;
    private int maxSize = 16, concurrencyLevel = 1;
    private float loadFactor = 0.75F;
    private int tick = 100;
    private Strength strength;
    private RemovalListener<K, V> removal;

    public CacheBuilder<K, V> executor(Executor executor) {
        requireNonNull(executor);
        this.executor = executor;
        return this;
    }
    public CacheBuilder<K, V> expireAfterAccess(long duration, TimeUnit unit) {
        requireNonNull(unit);
        this.expireAfterAccess = unit.toMillis(duration);
        return this;
    }
    public CacheBuilder<K, V> expireAfterWrite(long duration, TimeUnit unit) {
        requireNonNull(unit);
        this.expireAfterWrite = unit.toMillis(duration);
        return this;
    }
    public CacheBuilder<K, V> maxSize(int maxSize) {
        this.maxSize = maxSize;
        return this;
    }
    public CacheBuilder<K, V> concurrency(int concurrency) {
        this.concurrencyLevel = concurrency;
        return this;
    }
    public CacheBuilder<K, V> tick(int tick) {
        this.tick = tick;
        return this;
    }
    public CacheBuilder<K, V> loadFactor(float loadFactor) {
        this.loadFactor = loadFactor;
        return this;
    }
    public CacheBuilder<K, V> removal(RemovalListener<K, V> removal) {
        this.removal = removal;
        return this;
    }
    public CacheBuilder<K, V> softValues() {
        this.strength = Strength.SOFT;
        return this;
    }
    public CacheBuilder<K, V> weakValues() {
        this.strength = Strength.WEAK;
        return this;
    }
    public ConcurrentCache<K, V> build(AsyncCacheLoader<K, V> asyncCacheLoader) {
        return new ConcurrentCache<>(asyncCacheLoader, this);
    }

    public Executor getExecutor() {
        return executor;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }

    public int getTick() {
        return tick;
    }

    public long getExpireAfterAccess() {
        return expireAfterAccess;
    }
    public long getExpireAfterWrite() {
        return expireAfterWrite;
    }

    public Strength getStrength() {
        return strength;
    }

    public float getLoadFactor() {
        return loadFactor;
    }

    public RemovalListener<K, V> getRemoval() {
        return removal;
    }

    enum Strength {
        WEAK, SOFT
    }
    @FunctionalInterface
    public interface AsyncCacheLoader<K, V> {
        CompletableFuture<V> asyncLoad(K key, Executor executor);
    }
    @FunctionalInterface
    public interface RemovalListener<K, V> {
        CompletableFuture<Boolean> onRemoval(K key, CompletableFuture<V> v);
    }
}
