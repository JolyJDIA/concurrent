package jolyjdia.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.UnaryOperator;

/**
 * @param <V> The type of object
 * @author JolyJDIA
 */
public class AtomicVerifier<V> {
    private static final VarHandle FRESH;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            FRESH = l.findVarHandle(AtomicVerifier.class, "fresh", Object.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private final V initial;
    private volatile V fresh;

    public AtomicVerifier(V initial, V fresh) {
        this.initial = initial;
        this.fresh = fresh;
    }
    public AtomicVerifier(V initial) {
        this(initial, initial);
    }

    public final void setFresh(V newVal) {
        this.fresh = newVal;
    }

    public void retain() {}

    public final V getInitial() {
        return initial;
    }

    public final V getFresh() {
        return fresh;
    }

    public final void lazySetFresh(V newValue) {
        FRESH.setRelease(this, newValue);
    }

    public final boolean compareAndSetFresh(V expectedValue, V newValue) {
        return FRESH.compareAndSet(this, expectedValue, newValue);
    }
    public final boolean weakCompareAndSetFreshPlain(V expectedValue, V newValue) {
        return FRESH.weakCompareAndSetPlain(this, expectedValue, newValue);
    }
    public final V getAndSetFresh(V newValue) {
        return (V)FRESH.getAndSet(this, newValue);
    }

    public final V updateAndGetFresh(UnaryOperator<V> updateFunction) {
        V prev = fresh, next = null;
        for (boolean noNext = true;;) {
            if (noNext)
                next = updateFunction.apply(prev);
            if (FRESH.weakCompareAndSet(this, prev, next))
                return next;
            noNext = (prev != (prev = fresh));
        }
    }
    public final V getAndUpdateFresh(UnaryOperator<V> updateFunction) {
        V prev = fresh, next = null;
        for (boolean noNext = true;;) {
            if (noNext)
                next = updateFunction.apply(prev);
            if (FRESH.weakCompareAndSet(this, prev, next))
                return prev;
            noNext = (prev != (prev = fresh));
        }
    }

    @Override
    public String toString() {
        return "AtomicVerifier{" + "initial=" + initial + ", fresh=" + fresh + '}';
    }
}
