package org.dist.dbgossip;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class EndpointState {
    protected static final Logger logger = LoggerFactory.getLogger(EndpointState.class);

    private volatile HeartBeatState hbState;
    private final AtomicReference<Map<ApplicationState, VersionedValue>> applicationState;

    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    public EndpointState(HeartBeatState initialHbState)
    {
        this(initialHbState, new EnumMap<ApplicationState, VersionedValue>(ApplicationState.class));
    }

    EndpointState(HeartBeatState initialHbState, Map<ApplicationState, VersionedValue> states)
    {
        hbState = initialHbState;
        applicationState = new AtomicReference<Map<ApplicationState, VersionedValue>>(new EnumMap<>(states));
        updateTimestamp = System.nanoTime();
        isAlive = true;
    }


    public boolean isAlive()
    {
        return isAlive;
    }

    void markAlive()
    {
        isAlive = true;
    }

    void markDead()
    {
        isAlive = false;
    }

    public long getUpdateTimestamp()
    {
        return updateTimestamp;
    }

    public void addApplicationState(ApplicationState key, VersionedValue value)
    {
        addApplicationStates(Collections.singletonMap(key, value));
    }

    public void addApplicationStates(Map<ApplicationState, VersionedValue> values)
    {
        addApplicationStates(values.entrySet());
    }

    public void addApplicationStates(Set<Map.Entry<ApplicationState, VersionedValue>> values)
    {
        while (true)
        {
            Map<ApplicationState, VersionedValue> orig = applicationState.get();
            Map<ApplicationState, VersionedValue> copy = new EnumMap<>(orig);

            for (Map.Entry<ApplicationState, VersionedValue> value : values)
                copy.put(value.getKey(), value.getValue());

            if (applicationState.compareAndSet(orig, copy))
                return;
        }
    }

    HeartBeatState getHeartBeatState()
    {
        return hbState;
    }

    public Set<Map.Entry<ApplicationState, VersionedValue>> states()
    {
        return applicationState.get().entrySet();
    }
}
