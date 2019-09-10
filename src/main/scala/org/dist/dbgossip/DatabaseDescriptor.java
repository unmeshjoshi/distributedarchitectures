package org.dist.dbgossip;

public class DatabaseDescriptor {
    public static InetAddressAndPort[] getSeeds() {
        return new InetAddressAndPort[]{new InetAddressAndPort(new Networks().ipv4Address(), 8080)};
    }

    public static String getPartitionerName() {
        return RandomPartitioner.class.getCanonicalName();
    }

    public static String getClusterName() {
        return "Test Cluster";
    }
}
