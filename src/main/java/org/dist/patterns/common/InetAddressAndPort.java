package org.dist.patterns.common;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class InetAddressAndPort implements Comparable<InetAddressAndPort> {
    public final InetAddress address;
    public final Integer port;

    public InetAddressAndPort(InetAddress address, Integer port) {

        this.address = address;
        this.port = port;
    }

    public static InetAddressAndPort create(String hostIp, Integer port)
    {
        try {
            return new InetAddressAndPort(InetAddress.getByName(hostIp), port);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public InetAddress getAddress() {
        return address;
    }

    public Integer getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InetAddressAndPort that = (InetAddressAndPort) o;
        return Objects.equals(address, that.address) &&
                Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }

    @Override
    public String toString() {
        return "InetAddressAndPort{" +
                "address=" + address +
                ", port=" + port +
                '}';
    }

    @Override
    public int compareTo(InetAddressAndPort other) {
        int result = this.address.getHostAddress().compareTo(other.address.getHostAddress());
        if (result == 0) {
            result = this.port.compareTo(other.port);
        }
        return result;
    }
}
