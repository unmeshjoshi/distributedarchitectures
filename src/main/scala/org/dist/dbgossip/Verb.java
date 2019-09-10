/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dist.dbgossip;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.dist.dbgossip.Stage.GOSSIP;

/**
 * Note that priorities except P0 are presently unused.  P0 corresponds to urgent, i.e. what used to be the "Gossip" connection.
 */
public enum Verb
{

    GOSSIP_DIGEST_SYN    (14, Priority.P0, Verb::getTimeout,     GOSSIP,            () -> GossipDigestSynVerbHandler.instance                      ),
    GOSSIP_DIGEST_ACK    (15, Priority.P0, Verb::getTimeout,     GOSSIP,            () -> GossipDigestAckVerbHandler.instance                      ),
    GOSSIP_DIGEST_ACK2   (16, Priority.P0, Verb::getTimeout,     GOSSIP,            () -> GossipDigestAck2VerbHandler.instance                     )



      // largest used ID: 99
    ;



    public static long getTimeout(TimeUnit unit) {
        return 10000L;
    }

    public static final List<Verb> VERBS = ImmutableList.copyOf(Verb.values());
    private final Supplier<? extends IVerbHandler<?>> handler;

    public boolean isResponse()
    {
        return false;
    }

    public long expiresAtNanos(long nowNanos)
    {
        return nowNanos + expiresAfterNanos();
    }

    public long expiresAfterNanos()
    {
        return expiration.applyAsLong(NANOSECONDS);
    }

    public enum Priority
    {
        P0,  // sends on the urgent connection (i.e. for Gossip, Echo)
    }

    public final int id;
    public final Priority priority;
    public final Stage stage;


    final Verb responseVerb;

    private final ToLongFunction<TimeUnit> expiration;


    /**
     * Verbs it's okay to drop if the request has been queued longer than the request timeout.  These
     * all correspond to client requests or something triggered by them; we don't want to
     * drop internal messages like bootstrap or repair notifications.
     */
    Verb(int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVerbHandler<?>> handler)
    {
        this(id, priority, expiration, stage, handler, null);
    }

    Verb(int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVerbHandler<?>> handler, Verb responseVerb)
    {
        this.stage = stage;
        if (id < 0)
            throw new IllegalArgumentException("Verb id must be non-negative, got " + id + " for verb " + name());

        this.id = id;
        this.priority = priority;
        this.handler = handler;
        this.responseVerb = responseVerb;
        this.expiration = expiration;
    }



    private static final Verb[] idToVerbMap;

    static
    {
        Verb[] verbs = values();
        int max = -1;
        for (Verb v : verbs)
            max = Math.max(v.id, max);

        Verb[] idMap = new Verb[max + 1];
        for (Verb v : verbs)
        {
            if (idMap[v.id] != null)
                throw new IllegalArgumentException("cannot have two verbs that map to the same id: " + v + " and " + idMap[v.id]);
            idMap[v.id] = v;
        }

        idToVerbMap = idMap;
    }

    static Verb fromId(int id)
    {
        Verb verb = id >= 0 && id < idToVerbMap.length ? idToVerbMap[id] : null;
        if (verb == null)
            throw new IllegalArgumentException("Unknown verb id " + id);
        return verb;
    }
}