package jolyjdia.util.concurrent.cache;

import sun.misc.Unsafe;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class JConcurrentHashMap<K,V> {
    private static final int RESIZE_STAMP_BITS = 16;
    private static final int MAX_RESIZERS = (1 << (32 - RESIZE_STAMP_BITS)) - 1;
    static final int TREEBIN   = -2; // hash for roots of trees
    private static final int RESIZE_STAMP_SHIFT = 32 - RESIZE_STAMP_BITS;
    static final int RESERVED  = -3; // hash for transient reservations
    private static final int MAXIMUM_CAPACITY = 1 << 30;
    static final int NCPU = Runtime.getRuntime().availableProcessors();
    private static final int MIN_TRANSFER_STRIDE = 16;
    static final int MOVED     = -1; // hash for forwarding nodes
    static final int HASH_BITS = 0x7fffffff; // usable bits of normal node hash
    private static final int DEFAULT_CAPACITY = 16;
    volatile Node<K, V>[] table;
    transient volatile Node<K,V>[] nextTable;

    final AtomicInteger sizeCtl = new AtomicInteger();
    final AtomicInteger transferIndex = new AtomicInteger();
    final LongAdder count = new LongAdder();

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
                CompletableFuture<V> oldVal = null; CompletableFuture<Boolean> rem = null;
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
                                        rem = e.removal;
                                        e.removal = null;
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
                if (rem != null && !rem.isDone()) {
                    rem.cancel(true);
                }
                if (binCount != 0) {
                    if (oldVal != null)
                        return oldVal;
                    break;
                }
            }
        }
        addCount(1, binCount);
        return null;
    }
    public void addCount(long scale, int check) {
        count.add(scale);

        if (check >= 0) {
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
                s = count.intValue();
            }
        }
    }
    private void transfer(Node<K,V>[] tab, Node<K,V>[] nextTab) {
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
        ForwardingNode<K,V> fwd = new ForwardingNode<>(nextTab);
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
    public CompletableFuture<V> compute(K key,
                     BiFunction<? super K, CompletableFuture<V>, CompletableFuture<V>> remappingFunction) {
        if (key == null || remappingFunction == null)
            throw new NullPointerException();
        int h = spread(key.hashCode());
        CompletableFuture<V> val = null;
        int delta = 0;
        int binCount = 0;
        for (Node<K,V>[] tab = table;;) {
            Node<K,V> f; int n, i, fh;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                Node<K,V> r = new ReservationNode<>();
                synchronized (r) {
                    if (casTabAt(tab, i, null, r)) {
                        binCount = 1;
                        Node<K,V> node = null;
                        try {
                            if ((val = remappingFunction.apply(key, null)) != null) {
                                delta = 1;
                                node = new Node<>(h, key, val);
                            }
                        } finally {
                            setTabAt(tab, i, node);
                        }
                    }
                }
                if (binCount != 0)
                    break;
            }
            else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                CompletableFuture<Boolean> rem = null;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<K,V> e = f, pred = null;; ++binCount) {
                                K ek;
                                if (e.hash == h && ((ek = e.key) == key || (key.equals(ek)))) {
                                    val = remappingFunction.apply(key, e.val);
                                    if (val != null) {
                                        e.val = val;
                                        e.refresh = System.currentTimeMillis();
                                        rem = e.removal;
                                        e.removal = null;
                                    } else {
                                        delta = -1;
                                        Node<K,V> en = e.next;
                                        if (pred != null)
                                            pred.next = en;
                                        else
                                            setTabAt(tab, i, en);
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null) {
                                    val = remappingFunction.apply(key, null);
                                    if (val != null) {
                                        if (pred.next != null)
                                            throw new IllegalStateException("Recursive update");
                                        delta = 1;
                                        pred.next = new Node<K,V>(h, key, val);
                                    }
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            binCount = 1;
                            TreeBin<K,V> t = (TreeBin<K,V>)f;
                            TreeNode<K,V> r, p;
                            if ((r = t.root) != null)
                                p = r.findTreeNode(h, key, null);
                            else
                                p = null;
                            CompletableFuture<V> pv = (p == null) ? null : p.val;
                            val = remappingFunction.apply(key, pv);
                            if (val != null) {
                                if (p != null) {
                                    p.val = val;
                                    p.refresh = System.currentTimeMillis();
                                    rem = p.removal;
                                    p.removal = null;
                                } else {
                                    delta = 1;
                                    t.putTreeVal(h, key, val);
                                }
                            }
                            else if (p != null) {
                                delta = -1;
                                if (t.removeTreeNode(p))
                                    setTabAt(tab, i, untreeify(t.first));
                            }
                        }
                    }
                    if (rem != null && !rem.isDone()) {
                        rem.cancel(true);
                    }
                }
                if (binCount != 0) {
                    break;
                }
            }
        }
        if (delta != 0) {
            addCount(delta, binCount);
        }
        return val;
    }
    static <K,V> Node<K,V> untreeify(Node<K,V> b) {
        Node<K,V> hd = null, tl = null;
        for (Node<K,V> q = b; q != null; q = q.next) {
            Node<K,V> p = new Node<>(q.hash, q.key, q.val);
            if (tl == null)
                hd = p;
            else
                tl.next = p;
            tl = p;
        }
        return hd;
    }
    public void removeAfter(Node<K, V> f, Supplier<CompletableFuture<Boolean>> removal) {
        K key = f.key;
        int h = spread(key.hashCode());
        for (Node<K,V>[] tab = table;;) {
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0) {
                tab = initTable();
            } else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                return;
            } else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                Node<K, V>[] finalTab = tab;
                Node<K, V> finalF = f;
                removal.get().thenApply(x -> {
                    int delta;
                    int binCount0;
                    synchronized (finalF) {
                        if (tabAt(finalTab, i) != finalF) {
                            return x;
                        }
                        if (fh >= 0) {
                            binCount0 = 1;
                            for (Node<K, V> pred = null; ; ++binCount0) {
                                K ek;
                                if (finalF.hash == h && ((ek = finalF.key) == key || (key.equals(ek)))) {
                                    if (!x) {
                                        finalF.removal = null;
                                        finalF.refresh = System.currentTimeMillis();
                                    } else {
                                        delta = -1;
                                        Node<K, V> en = finalF.next;
                                        if (pred != null)
                                            pred.next = en;
                                        else
                                            setTabAt(finalTab, i, en);
                                        addCount(delta, binCount0);
                                        break;
                                    }
                                }
                                pred = finalF;
                            }
                        }
                    }
                    return x;
                });
            }
        }
    }
    public CompletableFuture<Boolean> removeAfter(K key, Function<Node<K, V>, CompletableFuture<Boolean>> removal) {
        if (key == null) throw new NullPointerException();
        int h = spread(key.hashCode());
        for (Node<K,V>[] tab = table;;) {
            Node<K,V> f; int n, i, fh;
            if (tab == null || (n = tab.length) == 0) {
                tab = initTable();
            } else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                return CompletableFuture.completedFuture(null);
            } else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                Node<K, V>[] finalTab = tab;
                return removal.apply(f).thenApply(x -> {
                    int delta;
                    int binCount0;
                    synchronized (f) {
                        if (tabAt(finalTab, i) != f) {
                            return x;
                        }
                        if (fh >= 0) {
                            binCount0 = 1;
                            for (Node<K, V> pred = null; ; ++binCount0) {
                                K ek;
                                if (f.hash == h && ((ek = f.key) == key || (key.equals(ek)))) {
                                    if (!x) {
                                        f.removal = null;
                                        f.refresh = System.currentTimeMillis();
                                    } else {
                                        delta = -1;
                                        Node<K, V> en = f.next;
                                        if (pred != null)
                                            pred.next = en;
                                        else
                                            setTabAt(finalTab, i, en);
                                        addCount(delta, binCount0);
                                        break;
                                    }
                                }
                                pred = f;
                            }
                        }
                    }
                    return x;
                });
            }
        }
    }
    static int resizeStamp(int n) {
        return Integer.numberOfLeadingZeros(n) | (1 << (RESIZE_STAMP_BITS - 1));
    }
    final Node<K,V>[] helpTransfer(Node<K,V>[] tab, Node<K,V> f) {
        Node<K,V>[] nextTab; int sc;
        if (tab != null && (f instanceof ForwardingNode) &&
                (nextTab = ((ForwardingNode<K,V>)f).nextTable) != null) {
            int rs = resizeStamp(tab.length) << RESIZE_STAMP_SHIFT;
            while (nextTab == nextTable && table == tab &&
                    (sc = sizeCtl.get()) < 0) {
                if (sc == rs + MAX_RESIZERS || sc == rs + 1 ||
                        transferIndex.get() <= 0)
                    break;
                if (sizeCtl.compareAndSet(sc, sc + 1)) {
                    transfer(tab, nextTab);
                    break;
                }
            }
            return nextTab;
        }
        return table;
    }


    final CompletableFuture<V> replaceNode(Object key, CompletableFuture<V> value, Object cv) {
        int hash = spread(key.hashCode());
        for (Node<K,V>[] tab = table;;) {
            Node<K,V> f; int n, i, fh;
            if (tab == null || (n = tab.length) == 0 ||
                    (f = tabAt(tab, i = (n - 1) & hash)) == null)
                break;
            else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                CompletableFuture<V> oldVal = null; CompletableFuture<Boolean> rem = null;
                boolean validated = false;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            validated = true;
                            for (Node<K,V> e = f, pred = null;;) {
                                K ek;
                                if (e.hash == hash &&
                                        ((ek = e.key) == key || (key.equals(ek)))) {
                                    CompletableFuture<V> ev = e.val;
                                    if (cv == null || cv == ev || (cv.equals(ev))) {
                                        oldVal = ev;
                                        if (value != null) {
                                            e.val = value;
                                            e.refresh = System.currentTimeMillis();
                                            rem = e.removal;
                                            e.removal = null;
                                        } else if (pred != null)
                                            pred.next = e.next;
                                        else
                                            setTabAt(tab, i, e.next);
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null)
                                    break;
                            }
                        }
                    }
                }
                if (rem != null && !rem.isDone()) {
                    rem.cancel(true);
                }
                if (validated) {
                    if (oldVal != null) {
                        if (value == null)
                            addCount(-1L, -1);
                        return oldVal;
                    }
                    break;
                }
            }
        }
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
    
    public void forEach(Consumer<Node<K, V>> action) {
        if (action == null) throw new NullPointerException();
        Node<K,V>[] t;
        if ((t = table) != null) {
            Traverser<K,V> it = new Traverser<>(t, t.length, 0, t.length);
            for (Node<K,V> p; (p = it.advance()) != null; ) {
                action.accept(p);
            }
        }
    }
    /* ---------------- TreeNodes -------------- */

    /**
     * Nodes for use in TreeBins.
     */
    static final class TreeNode<K,V> extends Node<K,V> {
        TreeNode<K,V> parent;  // red-black tree links
        TreeNode<K,V> left;
        TreeNode<K,V> right;
        TreeNode<K,V> prev;    // needed to unlink next upon deletion
        boolean red;

        TreeNode(int hash, K key, CompletableFuture<V> val, Node<K,V> next,
                 TreeNode<K,V> parent) {
            super(hash, key, val, next);
            this.parent = parent;
        }

        Node<K,V> find(int h, Object k) {
            return findTreeNode(h, k, null);
        }

        /**
         * Returns the TreeNode (or null if not found) for the given key
         * starting at given root.
         */
        final TreeNode<K,V> findTreeNode(int h, Object k, Class<?> kc) {
            if (k != null) {
                TreeNode<K,V> p = this;
                do {
                    int ph, dir; K pk; TreeNode<K,V> q;
                    TreeNode<K,V> pl = p.left, pr = p.right;
                    if ((ph = p.hash) > h)
                        p = pl;
                    else if (ph < h)
                        p = pr;
                    else if ((pk = p.key) == k || (k.equals(pk)))
                        return p;
                    else if (pl == null)
                        p = pr;
                    else if (pr == null)
                        p = pl;
                    else if ((kc != null ||
                            (kc = comparableClassFor(k)) != null) &&
                            (dir = compareComparables(kc, k, pk)) != 0)
                        p = (dir < 0) ? pl : pr;
                    else if ((q = pr.findTreeNode(h, k, kc)) != null)
                        return q;
                    else
                        p = pl;
                } while (p != null);
            }
            return null;
        }
    }
    static final class TreeBin<K,V> extends Node<K,V> {
        TreeNode<K,V> root;
        volatile TreeNode<K,V> first;
        volatile Thread waiter;
        volatile int lockState;
        // values for lockState
        static final int WRITER = 1; // set while holding write lock
        static final int WAITER = 2; // set when waiting for write lock
        static final int READER = 4; // increment value for setting read lock

        /**
         * Tie-breaking utility for ordering insertions when equal
         * hashCodes and non-comparable. We don't require a total
         * order, just a consistent insertion rule to maintain
         * equivalence across rebalancings. Tie-breaking further than
         * necessary simplifies testing a bit.
         */
        static int tieBreakOrder(Object a, Object b) {
            int d;
            if (a == null || b == null ||
                    (d = a.getClass().getName().
                            compareTo(b.getClass().getName())) == 0)
                d = (System.identityHashCode(a) <= System.identityHashCode(b) ?
                        -1 : 1);
            return d;
        }

        /**
         * Creates bin with initial set of nodes headed by b.
         */
        TreeBin(TreeNode<K,V> b) {
            super(TREEBIN, null, null);
            this.first = b;
            TreeNode<K,V> r = null;
            for (TreeNode<K,V> x = b, next; x != null; x = next) {
                next = (TreeNode<K,V>)x.next;
                x.left = x.right = null;
                if (r == null) {
                    x.parent = null;
                    x.red = false;
                    r = x;
                }
                else {
                    K k = x.key;
                    int h = x.hash;
                    Class<?> kc = null;
                    for (TreeNode<K,V> p = r;;) {
                        int dir, ph;
                        K pk = p.key;
                        if ((ph = p.hash) > h)
                            dir = -1;
                        else if (ph < h)
                            dir = 1;
                        else if ((kc == null &&
                                (kc = comparableClassFor(k)) == null) ||
                                (dir = compareComparables(kc, k, pk)) == 0)
                            dir = tieBreakOrder(k, pk);
                        TreeNode<K,V> xp = p;
                        if ((p = (dir <= 0) ? p.left : p.right) == null) {
                            x.parent = xp;
                            if (dir <= 0)
                                xp.left = x;
                            else
                                xp.right = x;
                            r = balanceInsertion(r, x);
                            break;
                        }
                    }
                }
            }
            this.root = r;
            assert checkInvariants(root);
        }

        /**
         * Acquires write lock for tree restructuring.
         */
        private void lockRoot() {
            if (!LOCKSTATE.compareAndSet(this, 0, WRITER))
                contendedLock(); // offload to separate method
        }

        /**
         * Releases write lock for tree restructuring.
         */
        private void unlockRoot() {
            lockState = 0;
        }

        /**
         * Possibly blocks awaiting root lock.
         */
        private void contendedLock() {
            boolean waiting = false;
            for (int s;;) {
                if (((s = lockState) & ~WAITER) == 0) {
                    if (LOCKSTATE.compareAndSet(this, s, WRITER)) {
                        if (waiting)
                            waiter = null;
                        return;
                    }
                }
                else if ((s & WAITER) == 0) {
                    if (LOCKSTATE.compareAndSet(this, s, s | WAITER)) {
                        waiting = true;
                        waiter = Thread.currentThread();
                    }
                }
                else if (waiting)
                    LockSupport.park(this);
            }
        }

        /**
         * Returns matching node or null if none. Tries to search
         * using tree comparisons from root, but continues linear
         * search when lock not available.
         */
        final Node<K,V> find(int h, Object k) {
            if (k != null) {
                for (Node<K,V> e = first; e != null; ) {
                    int s; K ek;
                    if (((s = lockState) & (WAITER|WRITER)) != 0) {
                        if (e.hash == h &&
                                ((ek = e.key) == k || (k.equals(ek))))
                            return e;
                        e = e.next;
                    }
                    else if (LOCKSTATE.compareAndSet(this, s, s + READER)) {
                        TreeNode<K,V> r, p;
                        try {
                            p = ((r = root) == null ? null :
                                    r.findTreeNode(h, k, null));
                        } finally {
                            Thread w;
                            if ((int)LOCKSTATE.getAndAdd(this, -READER) ==
                                    (READER|WAITER) && (w = waiter) != null)
                                LockSupport.unpark(w);
                        }
                        return p;
                    }
                }
            }
            return null;
        }

        /**
         * Finds or adds a node.
         * @return null if added
         */
        final TreeNode<K,V> putTreeVal(int h, K k, CompletableFuture<V> v) {
            Class<?> kc = null;
            boolean searched = false;
            for (TreeNode<K,V> p = root;;) {
                int dir, ph; K pk;
                if (p == null) {
                    first = root = new TreeNode<K,V>(h, k, v, null, null);
                    break;
                }
                else if ((ph = p.hash) > h)
                    dir = -1;
                else if (ph < h)
                    dir = 1;
                else if ((pk = p.key) == k || (k.equals(pk)))
                    return p;
                else if ((kc == null &&
                        (kc = comparableClassFor(k)) == null) ||
                        (dir = compareComparables(kc, k, pk)) == 0) {
                    if (!searched) {
                        TreeNode<K,V> q, ch;
                        searched = true;
                        if (((ch = p.left) != null &&
                                (q = ch.findTreeNode(h, k, kc)) != null) ||
                                ((ch = p.right) != null &&
                                        (q = ch.findTreeNode(h, k, kc)) != null))
                            return q;
                    }
                    dir = tieBreakOrder(k, pk);
                }

                TreeNode<K,V> xp = p;
                if ((p = (dir <= 0) ? p.left : p.right) == null) {
                    TreeNode<K,V> x, f = first;
                    first = x = new TreeNode<K,V>(h, k, v, f, xp);
                    if (f != null)
                        f.prev = x;
                    if (dir <= 0)
                        xp.left = x;
                    else
                        xp.right = x;
                    if (!xp.red)
                        x.red = true;
                    else {
                        lockRoot();
                        try {
                            root = balanceInsertion(root, x);
                        } finally {
                            unlockRoot();
                        }
                    }
                    break;
                }
            }
            assert checkInvariants(root);
            return null;
        }

        /**
         * Removes the given node, that must be present before this
         * call.  This is messier than typical red-black deletion code
         * because we cannot swap the contents of an interior node
         * with a leaf successor that is pinned by "next" pointers
         * that are accessible independently of lock. So instead we
         * swap the tree linkages.
         *
         * @return true if now too small, so should be untreeified
         */
        final boolean removeTreeNode(TreeNode<K,V> p) {
            TreeNode<K,V> next = (TreeNode<K,V>)p.next;
            TreeNode<K,V> pred = p.prev;  // unlink traversal pointers
            TreeNode<K,V> r, rl;
            if (pred == null)
                first = next;
            else
                pred.next = next;
            if (next != null)
                next.prev = pred;
            if (first == null) {
                root = null;
                return true;
            }
            if ((r = root) == null || r.right == null || // too small
                    (rl = r.left) == null || rl.left == null)
                return true;
            lockRoot();
            try {
                TreeNode<K,V> replacement;
                TreeNode<K,V> pl = p.left;
                TreeNode<K,V> pr = p.right;
                if (pl != null && pr != null) {
                    TreeNode<K,V> s = pr, sl;
                    while ((sl = s.left) != null) // find successor
                        s = sl;
                    boolean c = s.red; s.red = p.red; p.red = c; // swap colors
                    TreeNode<K,V> sr = s.right;
                    TreeNode<K,V> pp = p.parent;
                    if (s == pr) { // p was s's direct parent
                        p.parent = s;
                        s.right = p;
                    }
                    else {
                        TreeNode<K,V> sp = s.parent;
                        if ((p.parent = sp) != null) {
                            if (s == sp.left)
                                sp.left = p;
                            else
                                sp.right = p;
                        }
                        if ((s.right = pr) != null)
                            pr.parent = s;
                    }
                    p.left = null;
                    if ((p.right = sr) != null)
                        sr.parent = p;
                    if ((s.left = pl) != null)
                        pl.parent = s;
                    if ((s.parent = pp) == null)
                        r = s;
                    else if (p == pp.left)
                        pp.left = s;
                    else
                        pp.right = s;
                    replacement = Objects.requireNonNullElse(sr, p);
                }
                else replacement = Objects.requireNonNullElseGet(pl, () -> Objects.requireNonNullElse(pr, p));
                if (replacement != p) {
                    TreeNode<K,V> pp = replacement.parent = p.parent;
                    if (pp == null)
                        r = replacement;
                    else if (p == pp.left)
                        pp.left = replacement;
                    else
                        pp.right = replacement;
                    p.left = p.right = p.parent = null;
                }

                root = (p.red) ? r : balanceDeletion(r, replacement);

                if (p == replacement) {  // detach pointers
                    TreeNode<K,V> pp;
                    if ((pp = p.parent) != null) {
                        if (p == pp.left)
                            pp.left = null;
                        else if (p == pp.right)
                            pp.right = null;
                        p.parent = null;
                    }
                }
            } finally {
                unlockRoot();
            }
            assert checkInvariants(root);
            return false;
        }

        /* ------------------------------------------------------------ */
        // Red-black tree methods, all adapted from CLR

        static <K,V> TreeNode<K,V> rotateLeft(TreeNode<K,V> root,
                                                                TreeNode<K,V> p) {
            TreeNode<K,V> r, pp, rl;
            if (p != null && (r = p.right) != null) {
                if ((rl = p.right = r.left) != null)
                    rl.parent = p;
                if ((pp = r.parent = p.parent) == null)
                    (root = r).red = false;
                else if (pp.left == p)
                    pp.left = r;
                else
                    pp.right = r;
                r.left = p;
                p.parent = r;
            }
            return root;
        }

        static <K,V> TreeNode<K,V> rotateRight(TreeNode<K,V> root,
                                                                 TreeNode<K,V> p) {
            TreeNode<K,V> l, pp, lr;
            if (p != null && (l = p.left) != null) {
                if ((lr = p.left = l.right) != null)
                    lr.parent = p;
                if ((pp = l.parent = p.parent) == null)
                    (root = l).red = false;
                else if (pp.right == p)
                    pp.right = l;
                else
                    pp.left = l;
                l.right = p;
                p.parent = l;
            }
            return root;
        }

        static <K,V> TreeNode<K,V> balanceInsertion(TreeNode<K,V> root,
                                                                      TreeNode<K,V> x) {
            x.red = true;
            for (TreeNode<K,V> xp, xpp, xppl, xppr;;) {
                if ((xp = x.parent) == null) {
                    x.red = false;
                    return x;
                }
                else if (!xp.red || (xpp = xp.parent) == null)
                    return root;
                if (xp == (xppl = xpp.left)) {
                    if ((xppr = xpp.right) != null && xppr.red) {
                        xppr.red = false;
                        xp.red = false;
                        xpp.red = true;
                        x = xpp;
                    }
                    else {
                        if (x == xp.right) {
                            root = rotateLeft(root, x = xp);
                            xpp = (xp = x.parent) == null ? null : xp.parent;
                        }
                        if (xp != null) {
                            xp.red = false;
                            if (xpp != null) {
                                xpp.red = true;
                                root = rotateRight(root, xpp);
                            }
                        }
                    }
                }
                else {
                    if (xppl != null && xppl.red) {
                        xppl.red = false;
                        xp.red = false;
                        xpp.red = true;
                        x = xpp;
                    }
                    else {
                        if (x == xp.left) {
                            root = rotateRight(root, x = xp);
                            xpp = (xp = x.parent) == null ? null : xp.parent;
                        }
                        if (xp != null) {
                            xp.red = false;
                            if (xpp != null) {
                                xpp.red = true;
                                root = rotateLeft(root, xpp);
                            }
                        }
                    }
                }
            }
        }

        static <K,V> TreeNode<K,V> balanceDeletion(TreeNode<K,V> root,
                                                                     TreeNode<K,V> x) {
            for (TreeNode<K,V> xp, xpl, xpr;;) {
                if (x == null || x == root)
                    return root;
                else if ((xp = x.parent) == null) {
                    x.red = false;
                    return x;
                }
                else if (x.red) {
                    x.red = false;
                    return root;
                }
                else if ((xpl = xp.left) == x) {
                    if ((xpr = xp.right) != null && xpr.red) {
                        xpr.red = false;
                        xp.red = true;
                        root = rotateLeft(root, xp);
                        xpr = (xp = x.parent) == null ? null : xp.right;
                    }
                    if (xpr == null)
                        x = xp;
                    else {
                        TreeNode<K,V> sl = xpr.left, sr = xpr.right;
                        if ((sr == null || !sr.red) &&
                                (sl == null || !sl.red)) {
                            xpr.red = true;
                            x = xp;
                        }
                        else {
                            if (sr == null || !sr.red) {
                                sl.red = false;
                                xpr.red = true;
                                root = rotateRight(root, xpr);
                                xpr = (xp = x.parent) == null ?
                                        null : xp.right;
                            }
                            if (xpr != null) {
                                xpr.red = xp.red;
                                if ((sr = xpr.right) != null)
                                    sr.red = false;
                            }
                            if (xp != null) {
                                xp.red = false;
                                root = rotateLeft(root, xp);
                            }
                            x = root;
                        }
                    }
                }
                else { // symmetric
                    if (xpl != null && xpl.red) {
                        xpl.red = false;
                        xp.red = true;
                        root = rotateRight(root, xp);
                        xpl = (xp = x.parent) == null ? null : xp.left;
                    }
                    if (xpl == null)
                        x = xp;
                    else {
                        TreeNode<K,V> sl = xpl.left, sr = xpl.right;
                        if ((sl == null || !sl.red) &&
                                (sr == null || !sr.red)) {
                            xpl.red = true;
                            x = xp;
                        }
                        else {
                            if (sl == null || !sl.red) {
                                sr.red = false;
                                xpl.red = true;
                                root = rotateLeft(root, xpl);
                                xpl = (xp = x.parent) == null ?
                                        null : xp.left;
                            }
                            if (xpl != null) {
                                xpl.red = xp.red;
                                if ((sl = xpl.left) != null)
                                    sl.red = false;
                            }
                            if (xp != null) {
                                xp.red = false;
                                root = rotateRight(root, xp);
                            }
                            x = root;
                        }
                    }
                }
            }
        }

        /**
         * Checks invariants recursively for the tree of Nodes rooted at t.
         */
        static <K,V> boolean checkInvariants(TreeNode<K,V> t) {
            TreeNode<K,V> tp = t.parent, tl = t.left, tr = t.right,
                    tb = t.prev, tn = (TreeNode<K,V>)t.next;
            if (tb != null && tb.next != t)
                return false;
            if (tn != null && tn.prev != t)
                return false;
            if (tp != null && t != tp.left && t != tp.right)
                return false;
            if (tl != null && (tl.parent != t || tl.hash > t.hash))
                return false;
            if (tr != null && (tr.parent != t || tr.hash < t.hash))
                return false;
            if (t.red && tl != null && tl.red && tr != null && tr.red)
                return false;
            if (tl != null && !checkInvariants(tl))
                return false;
            return tr == null || checkInvariants(tr);
        }
        private static final VarHandle LOCKSTATE;
        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                LOCKSTATE = l.findVarHandle(TreeBin.class, "lockState", int.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

    }
    private static int tableSizeFor(int c) {
        int n = -1 >>> Integer.numberOfLeadingZeros(c - 1);
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    /**
     * Returns x's Class if it is of the form "class C implements
     * Comparable<C>", else null.
     */
    static Class<?> comparableClassFor(Object x) {
        if (x instanceof Comparable) {
            Class<?> c; Type[] ts, as; ParameterizedType p;
            if ((c = x.getClass()) == String.class) // bypass checks
                return c;
            if ((ts = c.getGenericInterfaces()) != null) {
                for (Type t : ts) {
                    if ((t instanceof ParameterizedType) &&
                            ((p = (ParameterizedType)t).getRawType() ==
                                    Comparable.class) &&
                            (as = p.getActualTypeArguments()) != null &&
                            as.length == 1 && as[0] == c) // type arg is c
                        return c;
                }
            }
        }
        return null;
    }

    /**
     * Returns k.compareTo(x) if x matches kc (k's screened comparable
     * class), else 0.
     */
    @SuppressWarnings({"rawtypes","unchecked"}) // for cast to Comparable
    static int compareComparables(Class<?> kc, Object k, Object x) {
        return (x == null || x.getClass() != kc ? 0 :
                ((Comparable)k).compareTo(x));
    }
    static final class TableStack<K,V> {
        int length;
        int index;
        Node<K,V>[] tab;
        TableStack<K,V> next;
    }
    static class Traverser<K,V> {
        Node<K,V>[] tab;        // current table; updated if resized
        Node<K,V> next;         // the next entry to use
        TableStack<K,V> stack, spare; // to save/restore on ForwardingNodes
        int index;              // index of bin to use next
        int baseIndex;          // current index of initial table
        int baseLimit;          // index bound for initial table
        final int baseSize;     // initial table size

        Traverser(Node<K,V>[] tab, int size, int index, int limit) {
            this.tab = tab;
            this.baseSize = size;
            this.baseIndex = this.index = index;
            this.baseLimit = limit;
            this.next = null;
        }

        /**
         * Advances if possible, returning next valid node, or null if none.
         */
        final Node<K,V> advance() {
            Node<K,V> e;
            if ((e = next) != null)
                e = e.next;
            for (;;) {
                Node<K,V>[] t; int i, n;  // must use locals in checks
                if (e != null)
                    return next = e;
                if (baseIndex >= baseLimit || (t = tab) == null ||
                        (n = t.length) <= (i = index) || i < 0)
                    return next = null;
                if ((e = tabAt(t, i)) != null && e.hash < 0) {
                    if (e instanceof ForwardingNode) {
                        tab = ((ForwardingNode<K,V>)e).nextTable;
                        e = null;
                        pushState(t, i, n);
                        continue;
                    } else
                        e = null;
                }
                if (stack != null)
                    recoverState(n);
                else if ((index = i + baseSize) >= n)
                    index = ++baseIndex; // visit upper slots if present
            }
        }

        /**
         * Saves traversal state upon encountering a forwarding node.
         */
        private void pushState(Node<K,V>[] t, int i, int n) {
            TableStack<K,V> s = spare;  // reuse if possible
            if (s != null)
                spare = s.next;
            else
                s = new TableStack<>();
            s.tab = t;
            s.length = n;
            s.index = i;
            s.next = stack;
            stack = s;
        }

        /**
         * Possibly pops traversal state.
         *
         * @param n length of current table
         */
        private void recoverState(int n) {
            TableStack<K,V> s; int len;
            while ((s = stack) != null && (index += (len = s.length)) >= n) {
                n = len;
                index = s.index;
                tab = s.tab;
                s.tab = null;
                TableStack<K,V> next = s.next;
                s.next = spare; // save for reuse
                stack = next;
                spare = s;
            }
            if (s == null && (index += baseSize) >= n)
                index = ++baseIndex;
        }
    }

    /**
     * Base of key, value, and entry Iterators. Adds fields to
     * Traverser to support iterator.remove.
     */
    static class BaseIterator<K,V> extends Traverser<K,V> {
        final JConcurrentHashMap<K,V> map;
        Node<K,V> lastReturned;
        BaseIterator(Node<K,V>[] tab, int size, int index, int limit,
                     JConcurrentHashMap<K,V> map) {
            super(tab, size, index, limit);
            this.map = map;
            advance();
        }

        public final boolean hasNext() { return next != null; }
        public final boolean hasMoreElements() { return next != null; }

        public final void remove() {
            Node<K,V> p;
            if ((p = lastReturned) == null)
                throw new IllegalStateException();
            lastReturned = null;
            map.replaceNode(p.key, null, null);
        }
    }


    public static class Node<K, V> {
        public final long start = System.currentTimeMillis();
        final int hash;
        public final K key;
        public volatile CompletableFuture<V> val;
        public volatile CompletableFuture<Boolean> removal;
        public volatile long refresh = start;
        volatile Node<K, V> next;

        public Node(int hash, K key, CompletableFuture<V> val) {
            this.hash = hash;
            this.key = key;
            this.val = val;
        }

        public Node(int hash, K key, CompletableFuture<V> val, Node<K, V> next) {
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
    static final class ReservationNode<K,V> extends Node<K,V> {
        ReservationNode() {
            super(RESERVED, null, null);
        }

        Node<K,V> find(int h, Object k) {
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