package org.dist.patterns.common;

import java.util.Objects;

public class RequestOrResponse {
    private Integer requestId;
    private String messageBodyJson;
    private Integer correlationId;

    //for jackson
    private RequestOrResponse(){}

    public RequestOrResponse(Integer requestId, String messageBodyJson, Integer correlationId) {
        this.requestId = requestId;
        this.messageBodyJson = messageBodyJson;
        this.correlationId = correlationId;
    }

    public Integer getRequestId() {
        return requestId;
    }

    public String getMessageBodyJson() {
        return messageBodyJson;
    }

    public Integer getCorrelationId() {
        return correlationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestOrResponse that = (RequestOrResponse) o;
        return Objects.equals(requestId, that.requestId) &&
                Objects.equals(messageBodyJson, that.messageBodyJson) &&
                Objects.equals(correlationId, that.correlationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, messageBodyJson, correlationId);
    }

    @Override
    public String toString() {
        return "RequestOrResponse{" +
                "requestId=" + requestId +
                ", messageBodyJson='" + messageBodyJson + '\'' +
                ", correlationId=" + correlationId +
                '}';
    }
}
