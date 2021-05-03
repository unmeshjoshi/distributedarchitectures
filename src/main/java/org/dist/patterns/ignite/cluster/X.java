package org.dist.patterns.ignite.cluster;


public class X {
    public static <T extends Throwable> T cause(Throwable t, Class<T> cls) {
        if (t == null || cls == null)
            return null;

        for (Throwable th = t; th != null; th = th.getCause()) {
            if (cls.isAssignableFrom(th.getClass()))
                return (T)th;

            for (Throwable n : th.getSuppressed()) {
                T found = cause(n, cls);

                if (found != null)
                    return found;
            }

            if (th.getCause() == th)
                break;
        }

        return null;
    }

    public static boolean hasCause(Throwable t, Class<?>... cls) {
        return hasCause(t, null, cls);
    }


    public static boolean hasCause(Throwable t, String msg, Class<?>... cls) {
        if (t == null || F.isEmpty(cls))
            return false;

        assert cls != null;

        for (Throwable th = t; th != null; th = th.getCause()) {
            for (Class<?> c : cls) {
                if (c.isAssignableFrom(th.getClass())) {
                    if (msg != null) {
                        if (th.getMessage() != null && th.getMessage().contains(msg))
                            return true;
                        else
                            continue;
                    }

                    return true;
                }
            }

            for (Throwable n : th.getSuppressed()) {
                if (hasCause(n, msg, cls))
                    return true;
            }

            if (th.getCause() == th)
                break;
        }

        return false;
    }
}
