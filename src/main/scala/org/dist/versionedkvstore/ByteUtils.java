package org.dist.versionedkvstore;

import org.apache.commons.codec.binary.Hex;

public class ByteUtils {
    public static String toHexString(byte[] key) {
        return Hex.encodeHexString(key);
    }
}
