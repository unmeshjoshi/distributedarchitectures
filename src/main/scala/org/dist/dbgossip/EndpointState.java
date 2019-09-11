package org.dist.dbgossip;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class EndpointState {
    protected static final Logger logger = LoggerFactory.getLogger(EndpointState.class);

    private volatile HeartBeatState hbState;
    private AtomicReference<Map<ApplicationState, VersionedValue>> applicationState;

    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    //For json serdes
    public EndpointState() {
    }

    public EndpointState(HeartBeatState initialHbState) {
        this(initialHbState, new EnumMap<ApplicationState, VersionedValue>(ApplicationState.class));
    }

    EndpointState(HeartBeatState initialHbState, Map<ApplicationState, VersionedValue> states) {
        hbState = initialHbState;
        applicationState = new AtomicReference<Map<ApplicationState, VersionedValue>>(new EnumMap<>(states));
        updateTimestamp = System.nanoTime();
        isAlive = true;
    }


    void markAlive() {
        isAlive = true;
    }

    void markDead() {
        isAlive = false;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void addApplicationState(ApplicationState key, VersionedValue value) {
        addApplicationStates(Collections.singletonMap(key, value));
    }

    public void addApplicationStates(Map<ApplicationState, VersionedValue> values) {
        addApplicationStates(values.entrySet());
    }

    public void addApplicationStates(Set<Map.Entry<ApplicationState, VersionedValue>> values) {
        while (true) {
            Map<ApplicationState, VersionedValue> orig = applicationState.get();
            Map<ApplicationState, VersionedValue> copy = new EnumMap<>(orig);

            for (Map.Entry<ApplicationState, VersionedValue> value : values)
                copy.put(value.getKey(), value.getValue());

            if (applicationState.compareAndSet(orig, copy))
                return;
        }
    }

    HeartBeatState getHeartBeatState() {
        return hbState;
    }

    public Set<Map.Entry<ApplicationState, VersionedValue>> states() {
        return applicationState.get().entrySet();
    }


    public String serialize() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static EndpointState deserialize(String json) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        try {
            return mapper.readValue(json, EndpointState.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EndpointState that = (EndpointState) o;
        return updateTimestamp == that.updateTimestamp &&
                isAlive == that.isAlive &&
                hbState.equals(that.hbState) &&
                applicationState.get().equals(that.applicationState.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(hbState, applicationState, updateTimestamp, isAlive);
    }
}
