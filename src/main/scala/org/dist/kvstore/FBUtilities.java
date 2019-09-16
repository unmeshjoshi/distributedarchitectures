package org.dist.kvstore;

import org.dist.util.Networks;

import java.math.BigInteger;
import java.security.MessageDigest;

public class FBUtilities {

    public static BigInteger hash(String data)
    {
        byte[] result = hash("MD5", data.getBytes());
        BigInteger hash = new BigInteger(result);
        return hash.abs();
    }


    public static byte[] hash(String type, byte[] data)
    {
        byte[] result = null;
        try
        {
            MessageDigest messageDigest = MessageDigest.getInstance(type);
            result = messageDigest.digest(data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static InetAddressAndPort getBroadcastAddressAndPort() {
        return new InetAddressAndPort(new Networks().ipv4Address(), 8000);
    }
}
