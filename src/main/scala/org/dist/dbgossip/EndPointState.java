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

public class EndPointState {
    protected static final Logger logger = LoggerFactory.getLogger(EndPointState.class);

    private volatile HeartBeatState hbState;
    private Map<ApplicationState, VersionedValue> applicationState;

    /* fields below do not get serialized */
    private volatile boolean isAlive;

    //For json serdes
    public EndPointState() {
    }

    public EndPointState(HeartBeatState initialHbState) {
        this(initialHbState, new EnumMap<ApplicationState, VersionedValue>(ApplicationState.class));
    }

    EndPointState(HeartBeatState initialHbState, Map<ApplicationState, VersionedValue> states) {
        hbState = initialHbState;
        applicationState = new EnumMap<>(states);
        isAlive = true;
    }


    void markAlive() {
        isAlive = true;
    }

    void markDead() {
        isAlive = false;
    }

    public void addApplicationState(ApplicationState key, VersionedValue value) {
        addApplicationStates(Collections.singletonMap(key, value));
    }

    public void addApplicationStates(Map<ApplicationState, VersionedValue> values) {
        addApplicationStates(values.entrySet());
    }

    public void addApplicationStates(Set<Map.Entry<ApplicationState, VersionedValue>> values) {
        Map<ApplicationState, VersionedValue> copy = new EnumMap<ApplicationState, VersionedValue>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> value : values)
            copy.put(value.getKey(), value.getValue());

        this.applicationState = copy;
    }

    HeartBeatState getHeartBeatState() {
        return hbState;
    }

    public Set<Map.Entry<ApplicationState, VersionedValue>> states() {
        return applicationState.entrySet();
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

    public static EndPointState deserialize(String json) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        try {
            return mapper.readValue(json, EndPointState.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EndPointState that = (EndPointState) o;
        return  isAlive == that.isAlive &&
                hbState.equals(that.hbState) &&
                applicationState.equals(that.applicationState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hbState, applicationState, isAlive);
    }

    public Map<ApplicationState, VersionedValue> getApplicationState() {
        return applicationState;
    }
}
