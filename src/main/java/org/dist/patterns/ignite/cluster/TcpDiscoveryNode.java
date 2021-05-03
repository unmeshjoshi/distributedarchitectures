/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dist.patterns.ignite.cluster;

import com.google.common.base.Objects;
import org.dist.kvstore.InetAddressAndPort;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class TcpDiscoveryNode extends GridMetadataAwareAdapter implements ClusterNode,
    Comparable<TcpDiscoveryNode>, Externalizable {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     * Node ID.
     */
    private UUID id;

    /**
     * Consistent ID.
     */
    private Object consistentId;

    /**
     * Node attributes.
     */

    private Map<String, Object> attrs;

    /**
     * Internal discovery addresses as strings.
     */

    private Collection<InetAddressAndPort> addrs;

    /**
     * Internal discovery host names as strings.
     */
    private Collection<String> hostNames;

    /**
     *
     */

    private Collection<InetSocketAddress> sockAddrs;

    /**
     *
     */

    private int discPort;

    /**
     * Node metrics.
     */

    private volatile ClusterMetrics metrics;

    /** Node cache metrics. */

    /**
     * Node order in the topology.
     */
    private volatile long order;

    /**
     * Node order in the topology (internal).
     */
    private volatile long intOrder;

    /**
     * The most recent time when heartbeat message was received from the node.
     */

    private volatile long lastUpdateTime = System.currentTimeMillis();

    /**
     * The most recent time when node exchanged a message with a remote node.
     */
    private volatile long lastExchangeTime = System.currentTimeMillis();

    /** Metrics provider (transient). */

    /**
     * Visible flag (transient).
     */

    private boolean visible;

    /**
     * Grid local node flag (transient).
     */
    private boolean loc;

    /**
     * Version.
     */
    private int ver;

    /**
     * Alive check (used by clients).
     */

    private transient int aliveCheck;

    /**
     * Client router node ID.
     */

    private UUID clientRouterNodeId;

    /**
     *
     */

    private volatile transient InetSocketAddress lastSuccessfulAddr;
    private long lastUpdateTimeNanos;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public TcpDiscoveryNode() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param id           Node Id.
     * @param addrs        Addresses.
     * @param hostNames    Host names.
     * @param discPort     Port.
     * @param ver          Version.
     * @param consistentId Node consistent ID.
     */
    public TcpDiscoveryNode(UUID id,
                            Collection<InetAddressAndPort> addrs,
                            Collection<String> hostNames,
                            int discPort,
                            int ver,
                            Serializable consistentId) {
        assert id != null;

        this.id = id;

        List<InetAddressAndPort> sortedAddrs = new ArrayList<>(addrs);

        Collections.sort(sortedAddrs, (o1, o2) -> o1.toString().compareTo(o2.toString()));

        this.addrs = sortedAddrs;
        this.hostNames = hostNames;
        this.discPort = discPort;
        this.ver = ver;

        this.consistentId = consistentId != null ? consistentId : consistentId(sortedAddrs, discPort);

        sockAddrs = addresses().stream().map(s -> {
            return new InetSocketAddress(s.address(), s.port());
        }).collect(Collectors.toList());
    }

    public static String consistentId(Collection<InetAddressAndPort> addrs, int port) {

        StringBuilder sb = new StringBuilder();

        for (InetAddressAndPort addr : addrs)
            sb.append(addr.toString()).append(',');

        sb.delete(sb.length() - 1, sb.length());

        sb.append(':').append(port);

        return sb.toString();
    }

    /**
     * @return Last successfully connected address.
     */
    public InetSocketAddress lastSuccessfulAddress() {
        return lastSuccessfulAddr;
    }

    /**
     * @param lastSuccessfulAddr Last successfully connected address.
     */
    public void lastSuccessfulAddress(InetSocketAddress lastSuccessfulAddr) {
        this.lastSuccessfulAddr = lastSuccessfulAddr;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID id() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object consistentId() {
        return consistentId;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T attribute(String name) {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
//        if (IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS.equals(name))
//            return null;

        return (T) attrs.get(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> attributes() {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        return attrs;
    }

    /**
     * Sets node attributes.
     *
     * @param attrs Node attributes.
     */
    public void setAttributes(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    /**
     * Gets node attributes without filtering.
     *
     * @return Node attributes without filtering.
     */
    public Map<String, Object> getAttributes() {
        return attrs;
    }


    /**
     * @return Internal order.
     */
    public long internalOrder() {
        return intOrder;
    }

    /**
     * @param intOrder Internal order of the node.
     */
    public void internalOrder(long intOrder) {
        assert intOrder > 0;

        this.intOrder = intOrder;
    }

    /**
     * @return Order.
     */
    @Override
    public long order() {
        return order;
    }

    @Override
    public Integer version() {
        return 0;
    }

    /**
     * @param order Order of the node.
     */
    public void order(long order) {
        assert order > 0 : "Order is invalid: " + this;

        this.order = order;
    }


    /**
     * {@inheritDoc}
     * @return
     */
    @Override
    public Collection<InetAddressAndPort> addresses() {
        return addrs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLocal() {
        return loc;
    }

    /**
     * @param loc Grid local node flag.
     */
    public void local(boolean loc) {
        this.loc = loc;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDaemon() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> hostNames() {
        return hostNames;
    }

    /**
     * @return Discovery port.
     */
    public int discoveryPort() {
        return discPort;
    }

    /**
     * @return Addresses that could be used by discovery.
     */
    public Collection<InetSocketAddress> socketAddresses() {
        return sockAddrs;
    }

    /**
     * Gets node last update time.
     *
     * @return Time of the last heartbeat.
     */
    public long lastUpdateTime() {
        return lastUpdateTime;
    }

    /**
     * Sets node last update.
     *
     * @param lastUpdateTime Time of last metrics update.
     */
    public void lastUpdateTime(long lastUpdateTime) {
        assert lastUpdateTime > 0;

        this.lastUpdateTime = lastUpdateTime;
    }

    /**
     * Gets the last time a node exchanged a message with a remote node.
     *
     * @return Time in milliseconds.
     */
    public long lastExchangeTime() {
        return lastExchangeTime;
    }

    /**
     * Sets the last time a node exchanged a message with a remote node.
     *
     * @param lastExchangeTime Time in milliseconds.
     */
    public void lastExchangeTime(long lastExchangeTime) {
        this.lastExchangeTime = lastExchangeTime;
    }

    /**
     * Gets visible flag.
     *
     * @return {@code true} if node is in visible state.
     */
    public boolean visible() {
        return visible;
    }

    /**
     * Sets visible flag.
     *
     * @param visible {@code true} if node is in visible state.
     */
    public void visible(boolean visible) {
        this.visible = visible;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClient() {
        return clientRouterNodeId != null;
    }

    /**
     * Decrements alive check value and returns new one.
     *
     * @return Alive check value.
     */
    public int decrementAliveCheck() {
        assert isClient();

        return --aliveCheck;
    }

    /**
     * @param aliveCheck Alive check value.
     */
    public void aliveCheck(int aliveCheck) {
        assert isClient();

        this.aliveCheck = aliveCheck;
    }

    /**
     * @return Client router node ID.
     */
    public UUID clientRouterNodeId() {
        return clientRouterNodeId;
    }

    /**
     * @param clientRouterNodeId Client router node ID.
     */
    public void clientRouterNodeId(UUID clientRouterNodeId) {
        this.clientRouterNodeId = clientRouterNodeId;
    }

    /**
     * @param newId New node ID.
     */
    public void onClientDisconnected(UUID newId) {
        id = newId;
    }

    /**
     * @return Copy of local node for client reconnect request.
     */
    public TcpDiscoveryNode clientReconnectNode() {
        TcpDiscoveryNode node = new TcpDiscoveryNode(id, addrs, hostNames, discPort, ver,
                null);

        node.attrs = attrs;
        node.clientRouterNodeId = clientRouterNodeId;

        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TcpDiscoveryNode node) {
        if (node == null)
            return 1;

        int res = Long.compare(internalOrder(), node.internalOrder());

        if (res == 0) {
            assert id().equals(node.id()) : "Duplicate order [this=" + this + ", other=" + node + ']';

            res = id().compareTo(node.id());
        }

        return res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TcpDiscoveryNode that = (TcpDiscoveryNode) o;
        return discPort == that.discPort &&
                order == that.order &&
                intOrder == that.intOrder &&
                lastUpdateTime == that.lastUpdateTime &&
                lastExchangeTime == that.lastExchangeTime &&
                visible == that.visible &&
                loc == that.loc &&
                ver == that.ver &&
                aliveCheck == that.aliveCheck &&
                Objects.equal(id, that.id) &&
                Objects.equal(consistentId, that.consistentId) &&
                Objects.equal(attrs, that.attrs) &&
                Objects.equal(addrs, that.addrs) &&
                Objects.equal(hostNames, that.hostNames) &&
                Objects.equal(sockAddrs, that.sockAddrs) &&
                Objects.equal(metrics, that.metrics) &&
                Objects.equal(clientRouterNodeId, that.clientRouterNodeId) &&
                Objects.equal(lastSuccessfulAddr, that.lastSuccessfulAddr);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, consistentId, attrs, addrs, hostNames, sockAddrs, discPort, metrics, order, intOrder, lastUpdateTime, lastExchangeTime, visible, loc, ver, aliveCheck, clientRouterNodeId, lastSuccessfulAddr);
    }

    @Override
    public String toString() {
        return "TcpDiscoveryNode{" +
                "id=" + id +
                ", consistentId=" + consistentId +
                ", hostNames=" + hostNames +
                ", discPort=" + discPort +
                ", order=" + order +
                ", intOrder=" + intOrder +
                ", lastUpdateTime=" + lastUpdateTime +
                ", lastExchangeTime=" + lastExchangeTime +
                ", loc=" + loc +
                '}';
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    public void lastUpdateTimeNanos(long nanoTime) {
        this.lastUpdateTimeNanos = nanoTime;
    }
}