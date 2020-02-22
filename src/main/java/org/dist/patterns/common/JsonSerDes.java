package org.dist.patterns.common;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class JsonSerDes {

    public static String serialize(Object obj) {
        var objectMapper = new ObjectMapper();
        try {
            objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            return objectMapper.writeValueAsString(obj);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    public static <T> T deserialize(String json, Class<T> clazz) {
        return deserialize(json.getBytes(), clazz);
    }

    public static <T> T deserialize(byte[] json, Class<T> clazz) {
        try {
            var objectMapper = new ObjectMapper();
            objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            var module = new SimpleModule();
            objectMapper.registerModule(module);
            return objectMapper.readValue(json, clazz);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
