package org.dist.patterns.common;

import org.junit.Assert;
import org.junit.Test;

public class JsonSerDesTest {

    @Test
    public void serializesJavaObjects() {
        var request = new RequestOrResponse(0, "some content", 1);
        String serializedMessage = JsonSerDes.serialize(request);
        Assert.assertEquals(
                "{\"requestId\":0,\"messageBodyJson\":\"some content\",\"correlationId\":1}", serializedMessage);
    }

    @Test
    public void deSerializesJavaObjects() {
        var request = new RequestOrResponse(0, "some content", 1);
        String serializedMessage = JsonSerDes.serialize(request);
        Assert.assertEquals(
                "{\"requestId\":0,\"messageBodyJson\":\"some content\",\"correlationId\":1}", serializedMessage);

        Assert.assertEquals(request, JsonSerDes.deserialize(serializedMessage, RequestOrResponse.class));
    }
}