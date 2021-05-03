package org.dist.patterns.ignite.cluster;

import java.util.*;

public class GridFunc {
    private static final IgniteClosure IDENTITY = new IdentityClosure();

    private static final IgnitePredicate<Object> ALWAYS_FALSE = new AlwaysFalsePredicate<>();
    private static final IgnitePredicate<Object> ALWAYS_TRUE = new AlwaysTruePredicate<>();

    /**
     * Gets size of the given collection with provided optional predicates.
     *
     * @param c Collection to size.
     * @param p Optional predicates that filters out elements from count.
     * @param <T> Type of the iterator.
     * @return Number of elements in the collection for which all given predicates
     *      evaluates to {@code true}. If no predicates is provided - all elements are counted.
     */
    public static <T> int size( Collection<? extends T> c,  IgnitePredicate<? super T>... p) {
        return c == null || c.isEmpty() ? 0 : isEmpty(p) || isAlwaysTrue(p) ? c.size() : size(c.iterator(), p);
    }

    /**
     * Gets size of the given iterator with provided optional predicates. Iterator
     * will be traversed to get the count.
     *
     * @param it Iterator to size.
     * @param p Optional predicates that filters out elements from count.
     * @param <T> Type of the iterator.
     * @return Number of elements in the iterator for which all given predicates
     *      evaluates to {@code true}. If no predicates is provided - all elements are counted.
     */
    public static <T> int size( Iterator<? extends T> it,  IgnitePredicate<? super T>... p) {
        if (it == null)
            return 0;

        int n = 0;

        if (!isAlwaysFalse(p)) {
            while (it.hasNext()) {
                if (isAll(it.next(), p))
                    n++;
            }
        }

        return n;
    }

    @Deprecated
    public static <T> GridIterator<T> emptyIterator() {
        return new GridEmptyIterator<>();
    }


    public static <T1, T2> GridIterator<T2> iterator(final Iterable<? extends T1> c,
                                                     final IgniteClosure<? super T1, T2> trans, final boolean readOnly,
                                                      final IgnitePredicate<? super T1>... p) {
        A.notNull(c, "c", trans, "trans");

        if (isAlwaysFalse(p))
            return F.emptyIterator();

        return new TransformFilteringIterator<>(c.iterator(), trans, readOnly, p);
    }

    public static <T> GridIterator<T> iterator0(Iterable<? extends T> c, boolean readOnly,
                                                IgnitePredicate<? super T>... p) {
        return F.iterator(c, IDENTITY, readOnly, p);
    }
    @SafeVarargs
    public static <T> Collection<T> view( final Collection<T> c,
                                          final IgnitePredicate<? super T>... p) {
        if (isEmpty(c) || isAlwaysFalse(p))
            return Collections.emptyList();

        return isEmpty(p) || isAlwaysTrue(p) ? c : new PredicateCollectionView<>(c, p);
    }

    public static boolean isAlwaysTrue( IgnitePredicate[] p) {
        return p != null && p.length == 1 && isAlwaysTrue(p[0]);
    }

    @Deprecated
    public static boolean isAlwaysTrue(IgnitePredicate p) {
        return p == ALWAYS_TRUE;
    }

    @Deprecated
    public static boolean isAlwaysFalse(IgnitePredicate p) {
        return p == ALWAYS_FALSE;
    }

    @Deprecated
    public static boolean isAlwaysFalse( IgnitePredicate[] p) {
        return p != null && p.length == 1 && isAlwaysFalse(p[0]);
    }

    public static <T> boolean isEmpty( T[] c) {
        return c == null || c.length == 0;
    }

    public static <T> boolean isAll( T t,  IgnitePredicate<? super T>... p) {
        if (p != null)
            for (IgnitePredicate<? super T> r : p)
                if (r != null && !r.apply(t))
                    return false;

        return true;
    }

    /**
     * Gets first element from given collection or returns {@code null} if the collection is empty.
     *
     * @param c A collection.
     * @param <T> Type of the collection.
     * @return Collections' first element or {@code null} in case if the collection is empty.
     */
    public static <T> T first( Iterable<? extends T> c) {
        if (c == null)
            return null;

        if (c instanceof List)
            return first((List<? extends T>)c);

        Iterator<? extends T> it = c.iterator();

        return it.hasNext() ? it.next() : null;
    }

    /**
     * Gets first element from given list or returns {@code null} if list is empty.
     *
     * @param list List.
     * @return List' first element or {@code null} in case if list is empty.
     */
    public static <T> T first(List<? extends T> list) {
        if (list == null || list.isEmpty())
            return null;

        return list.get(0);
    }
    /**
     * Gets predicate that evaluates to {@code false} for given local node ID.
     *
     * @param locNodeId Local node ID.
     * @param <T> Type of the node.
     * @return Return {@code false} for the given local node ID.
     */
    public static <T extends ClusterNode> IgnitePredicate<T> remoteNodes(final UUID locNodeId) {
        return new HasNotEqualIdPredicate<>(locNodeId);
    }

    public static boolean isEmpty( Iterable<?> c) {
        return c == null || (c instanceof Collection<?> ? ((Collection<?>) c).isEmpty() : !c.iterator().hasNext());
    }

    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param c Collection to test.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean isEmpty( Collection<?> c) {
        return c == null || c.isEmpty();
    }

    /**
     * Tests if the given map is either {@code null} or empty.
     *
     * @param m Map to test.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean isEmpty( Map<?, ?> m) {
        return m == null || m.isEmpty();
    }
}
