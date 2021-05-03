package org.dist.patterns.ignite.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class IgniteUtils {

    public static <T extends R, R> List<R> arrayList(Collection<T> c,  IgnitePredicate<? super T>... p) {
        assert c != null;

        return arrayList(c.iterator(), c.size(), p);
    }

    /**
     * @param c Collection.
     * @return Resulting array list.
     */
    public static <T extends R, R> List<R> arrayList(Collection<T> c) {
        assert c != null;

        return new ArrayList<R>(c);
    }

    /**
     * @param c Collection.
     * @param cap Initial capacity.
     * @param p Optional filters.
     * @return Resulting array list.
     */
    public static <T extends R, R> List<R> arrayList(Iterator<T> c, int cap,
                                                      IgnitePredicate<? super T>... p) {
        assert c != null;
        assert cap >= 0;

        List<R> list = new ArrayList<>(cap);

        while (c.hasNext()) {
            T t = c.next();

            if (F.isAll(t, p))
                list.add(t);
        }

        return list;
    }
}
