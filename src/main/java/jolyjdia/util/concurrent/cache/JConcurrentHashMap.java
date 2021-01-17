package jolyjdia.util.concurrent.cache;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class JConcurrentHashMap<K,V> {
    private static final int RESIZE_STAMP_BITS = 16;
    private static final int MAX_RESIZERS = (1 << (32 - RESIZE_STAMP_BITS)) - 1;
    private static final int RESIZE_STAMP_SHIFT = 32 - RESIZE_STAMP_BITS;
    private static final int MAXIMUM_CAPACITY = 1 << 30;
    static final int NCPU = Runtime.getRuntime().availableProcessors();
    private static final int MIN_TRANSFER_STRIDE = 16;
    static final int MOVED     = -1; // hash for forwarding nodes
    static final int HASH_BITS = 0x7fffffff; // usable bits of normal node hash
    private static final int DEFAULT_CAPACITY = 16;
    private volatile Node<K, V>[] table;
    private transient volatile Node<K,V>[] nextTable;

    private final AtomicInteger sizeCtl = new AtomicInteger();
    private final AtomicInteger transferIndex = new AtomicInteger();
    private final LongAdder count = new LongAdder();

    public int size() {
        return count.intValue();
    }

    private Node<K,V>[] initTable() {
        Node<K,V>[] tab; int sc;
        while ((tab = table) == null || tab.length == 0) {
            if ((sc = sizeCtl.get()) < 0)
                Thread.yield(); // lost initialization race; just spin
            else if (sizeCtl.compareAndSet(sc, -1)) {
                try {
                    if ((tab = table) == null || tab.length == 0) {
                        int n = (sc > 0) ? sc : DEFAULT_CAPACITY;
                        @SuppressWarnings("unchecked")
                        Node<K,V>[] nt = (Node<K,V>[])new Node<?,?>[n];
                        table = tab = nt;
                        sc = n - (n >>> 2);
                    }
                } finally {
                    sizeCtl.set(sc);
                }
                break;
            }
        }
        return tab;
    }

    public CompletableFuture<V> get(Object key) {
        Node<K,V>[] tab; Node<K,V> e, p; int n, eh; K ek;
        int h = spread(key.hashCode());
        if ((tab = table) != null && (n = tab.length) > 0 &&
                (e = tabAt(tab, (n - 1) & h)) != null) {
            if ((eh = e.hash) == h) {
                if ((ek = e.key) == key || (key.equals(ek)))
                    return e.val;
            }
            else if (eh < 0)
                return (p = e.find(h, key)) != null ? p.val : null;
            while ((e = e.next) != null) {
                if (e.hash == h &&
                        ((ek = e.key) == key || (key.equals(ek))))
                    return e.val;
            }
        }
        return null;
    }

    public CompletableFuture<V> put(K key, CompletableFuture<V> value, boolean onlyIfAbsent) {
        if (key == null || value == null) throw new NullPointerException();
        int hash = spread(key.hashCode());
        int binCount = 0;
        for (Node<K,V>[] tab = table;;) {
            Node<K,V> f; int n, i, fh; K fk; CompletableFuture<V> fv;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & hash)) == null) {
                if (casTabAt(tab, i, null, new Node<>(hash, key, value)))
                    break;                   // no lock when adding to empty bin
            }
            else if ((fh = f.hash) == MOVED)
                tab = table;
               // tab = helpTransfer(tab, f);
            else if (onlyIfAbsent // check first node without acquiring lock
                    && fh == hash
                    && ((fk = f.key) == key || (key.equals(fk)))
                    && (fv = f.val) != null)
                return fv;
            else {
                CompletableFuture<V> oldVal = null;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<K,V> e = f;; ++binCount) {
                                K ek;
                                if (e.hash == hash && ((ek = e.key) == key || (key.equals(ek)))) {
                                    oldVal = e.val;
                                    if (!onlyIfAbsent) {
                                        e.val = value;
                                        e.refresh = System.currentTimeMillis();
                                    }
                                    break;
                                }
                                Node<K,V> pred = e;
                                if ((e = e.next) == null) {
                                    pred.next = new Node<>(hash, key, value);
                                    break;
                                }
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    if (oldVal != null)
                        return oldVal;
                    break;
                }
            }
        }
        count.add(1);
        if (binCount >= 0) {
            int s = count.intValue();
            Node<K, V>[] tab, nt;
            int n, sc;
            while (s >= (long) (sc = sizeCtl.get()) && (tab = table) != null &&
                    (n = tab.length) < MAXIMUM_CAPACITY) {
                int rs = resizeStamp(n) << RESIZE_STAMP_SHIFT;
                if (sc < 0) {
                    if (sc == rs + MAX_RESIZERS || sc == rs + 1 ||
                            (nt = nextTable) == null || transferIndex.get() <= 0)
                        break;
                    if (sizeCtl.compareAndSet(sc, sc + 1))
                        transfer(tab, nt);
                } else if (sizeCtl.compareAndSet(sc, rs + 2))
                    transfer(tab, null);
            }
        }
        return null;
    }
    private final void transfer(Node<K,V>[] tab, Node<K,V>[] nextTab) {
        int n = tab.length, stride;
        if ((stride = (NCPU > 1) ? (n >>> 3) / NCPU : n) < MIN_TRANSFER_STRIDE)
            stride = MIN_TRANSFER_STRIDE; // subdivide range
        if (nextTab == null) {            // initiating
            try {
                @SuppressWarnings("unchecked")
                Node<K,V>[] nt = (Node<K,V>[])new Node<?,?>[n << 1];
                nextTab = nt;
            } catch (Throwable ex) {      // try to cope with OOME
                sizeCtl.set(Integer.MAX_VALUE);
                return;
            }
            nextTable = nextTab;
            transferIndex.set(n);
        }
        int nextn = nextTab.length;
        ForwardingNode<K,V> fwd = new ForwardingNode<K,V>(nextTab);
        boolean advance = true;
        boolean finishing = false; // to ensure sweep before committing nextTab
        for (int i = 0, bound = 0;;) {
            Node<K,V> f; int fh;
            while (advance) {
                int nextIndex, nextBound;
                if (--i >= bound || finishing)
                    advance = false;
                else if ((nextIndex = transferIndex.get()) <= 0) {
                    i = -1;
                    advance = false;
                }
                else if (transferIndex.compareAndSet(nextIndex, 
                        nextBound = (nextIndex > stride ? nextIndex - stride : 0))) {
                    bound = nextBound;
                    i = nextIndex - 1;
                    advance = false;
                }
            }
            if (i < 0 || i >= n || i + n >= nextn) {
                int sc;
                if (finishing) {
                    nextTable = null;
                    table = nextTab;
                    sizeCtl.set((n << 1) - (n >>> 1));
                    return;
                }
                if (sizeCtl.compareAndSet(sc = sizeCtl.get(), sc - 1)) {
                    if ((sc - 2) != resizeStamp(n) << RESIZE_STAMP_SHIFT)
                        return;
                    finishing = advance = true;
                    i = n; // recheck before commit
                }
            }
            else if ((f = tabAt(tab, i)) == null)
                advance = casTabAt(tab, i, null, fwd);
            else if ((fh = f.hash) == MOVED)
                advance = true; // already processed
            else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        Node<K,V> ln, hn;
                        if (fh >= 0) {
                            int runBit = fh & n;
                            Node<K,V> lastRun = f;
                            for (Node<K,V> p = f.next; p != null; p = p.next) {
                                int b = p.hash & n;
                                if (b != runBit) {
                                    runBit = b;
                                    lastRun = p;
                                }
                            }
                            if (runBit == 0) {
                                ln = lastRun;
                                hn = null;
                            }
                            else {
                                hn = lastRun;
                                ln = null;
                            }
                            for (Node<K,V> p = f; p != lastRun; p = p.next) {
                                int ph = p.hash; K pk = p.key; CompletableFuture<V> pv = p.val;
                                if ((ph & n) == 0)
                                    ln = new Node<>(ph, pk, pv, ln);
                                else
                                    hn = new Node<>(ph, pk, pv, hn);
                            }
                            setTabAt(nextTab, i, ln);
                            setTabAt(nextTab, i + n, hn);
                            setTabAt(tab, i, fwd);
                            advance = true;
                        }
                    }
                }
            }
        }
    }
    static final int resizeStamp(int n) {
        return Integer.numberOfLeadingZeros(n) | (1 << (RESIZE_STAMP_BITS - 1));
    }

    public V remove(Object key) {
        if (key == null) throw new IllegalArgumentException();
        int hash = spread(key.hashCode());

        return null;
    }

    static int spread(int h) {
        return (h ^ (h >>> 16)) & HASH_BITS;
    }
    static <K,V> Node<K,V> tabAt(Node<K,V>[] tab, int i) {
        return (Node<K,V>)U.getObjectVolatile(tab, ((long)i << ASHIFT) + ABASE);
    }

    static <K,V> boolean casTabAt(Node<K,V>[] tab, int i,
                                        Node<K,V> c, Node<K,V> v) {
        return U.compareAndSwapObject(tab, ((long)i << ASHIFT) + ABASE, c, v);
    }

    static <K,V> void setTabAt(Node<K,V>[] tab, int i, Node<K,V> v) {
        U.putObjectVolatile(tab, ((long)i << ASHIFT) + ABASE, v);
    }
    

    private boolean isKeyEquals(Object key, int hash, Node<K, V> node) {
        return node.hash == hash &&
                node.key == key ||
                (node.key != null && node.key.equals(key));
    }

    private static class Node<K, V> {
        final long start = System.currentTimeMillis();
        final int hash;
        final K key;
        volatile CompletableFuture<V> val;
        volatile CompletableFuture<Boolean> removal;
        volatile long refresh = start;
        volatile Node<K, V> next;

        Node(int hash, K key, CompletableFuture<V> val) {
            this.hash = hash;
            this.key = key;
            this.val = val;
        }

        Node(int hash, K key, CompletableFuture<V> val, Node<K, V> next) {
            this(hash, key, val);
            this.next = next;
        }
        /**
         * Virtualized support for map.get(); overridden in subclasses.
         */
        Node<K,V> find(int h, Object k) {
            Node<K, V> e = this;
            if (k != null) {
                do {
                    K ek;
                    if (e.hash == h && ((ek = e.key) == k || (k.equals(ek))))
                        return e;
                } while ((e = e.next) != null);
            }
            return null;
        }
    }
    static final class ForwardingNode<K,V> extends Node<K,V> {
        final Node<K,V>[] nextTable;
        ForwardingNode(Node<K,V>[] tab) {
            super(MOVED, null, null);
            this.nextTable = tab;
        }

        Node<K,V> find(int h, Object k) {
            // loop to avoid arbitrarily deep recursion on forwarding nodes
            outer: for (Node<K,V>[] tab = nextTable;;) {
                Node<K,V> e; int n;
                if (k == null || tab == null || (n = tab.length) == 0 ||
                        (e = tabAt(tab, (n - 1) & h)) == null)
                    return null;
                for (;;) {
                    int eh; K ek;
                    if ((eh = e.hash) == h && ((ek = e.key) == k || (k.equals(ek))))
                        return e;
                    if (eh < 0) {
                        if (e instanceof ForwardingNode) {
                            tab = ((ForwardingNode<K,V>)e).nextTable;
                            continue outer;
                        }
                        else
                            return e.find(h, k);
                    }
                    if ((e = e.next) == null)
                        return null;
                }
            }
        }
    }

    /* ---------------- Volatile bucket array access -------------- */

    @SuppressWarnings("unchecked")
    // read array header node value by index
    private <K, V> Node<K, V> volatileGetNode(int i) {
        return (Node<K, V>) U.getObjectVolatile(table, ((long) i << ASHIFT) + ABASE);
    }

    // cas array header node value by index
    private <K, V> boolean compareAndSwapNode(int i, Node<K, V> expectedNode, Node<K, V> setNode) {
        return U.compareAndSwapObject(table, ((long) i << ASHIFT) + ABASE, expectedNode, setNode);
    }

    private static final sun.misc.Unsafe U;
    // Node[] header shift
    private static final long ABASE;
    // Node.class size shift
    private static final int ASHIFT;

    static {
        // get unsafe by reflection - it is illegal to use not in java lib
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            U = unsafeConstructor.newInstance();
        } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        Class<?> ak = Node[].class;

        ABASE = U.arrayBaseOffset(ak);
        int scale = U.arrayIndexScale(ak);
        ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
        System.out.println("ASHIFT = "+ASHIFT + " ABASE = "+ ABASE);
    }
    /* ---------------- Volatile bucket array access -------------- */

    @Override
    public String toString() {
        return Arrays.toString(table) + "\n" + table.length;
    }
}