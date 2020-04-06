package org.dist.patterns.singularupdatequeue;

import org.dist.kvstore.InetAddressAndPort;
import org.dist.patterns.common.RequestOrResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

enum RequestType {
    CREDIT,
    DEBIT,
    READ_BALANCE,
    VALIDATION_RESPONSE,
    VALIDATE_REQUEST

}

enum ResponseCode {
    SUCCESS,
    FAILURE;
}

class Request {
    int amount;
    RequestType requestType;
    ResponseCode responseCode = ResponseCode.SUCCESS;
    int correlationId = 0;

    public Request(int amount, RequestType requestType) {
        this(amount, requestType, 0);
    }

    public Request(int amount, RequestType requestType, int correlationId) {
        this.amount = amount;
        this.requestType = requestType;
        this.correlationId = correlationId;
    }
}

class Response {
    public static Response None = new Response();
    Map<InetAddressAndPort, RequestOrResponse> messages = new HashMap();
    int amount;

    public Response withMessage(RequestOrResponse request, InetAddressAndPort to) {
        this.messages.put(to, request);
        return this;
    }
    public Response withAmount(int amount) {
        this.amount = amount;
        return this;
    }

    public static Response getNone() {
        return None;
    }

    public Map<InetAddressAndPort, RequestOrResponse> getMessages() {
        return messages;
    }

    public int getAmount() {
        return amount;
    }
}


public class Account {
    Map<Integer, Request> inflightMessages = new HashMap<>();
    InetAddressAndPort validationServiceIp = InetAddressAndPort.create("10.10.10.10", 9000);
    private int correlationId = 0;

    private int amount;

    SingularUpdateQueue<Request, Response> queue
            = new SingularUpdateQueue<Request, Response>((request) -> {
       if (request.requestType == RequestType.CREDIT) {

           return askToMakeValidationCall(request);

       } else if (request.requestType == RequestType.VALIDATION_RESPONSE) {

           return handleServiceResponse(request);
       }

       return Response.None;
    });
    {
        queue.start();
    }

    private Response askToMakeValidationCall(Request request) {
        int messageId = correlationId++;
        inflightMessages.put(messageId, request);

        RequestOrResponse validationRequest = new RequestOrResponse(1, "", messageId);
        return new Response().withMessage(validationRequest, validationServiceIp);
    }

    private Response handleServiceResponse(Request request) {
        if (request.responseCode == ResponseCode.SUCCESS) {
            Request request1 = inflightMessages.get(request.correlationId);
            creditOrDebit(request1);
            inflightMessages.remove(request.correlationId);
            return new Response().withAmount(this.amount);
        }
        return Response.None;
    }

    private void creditOrDebit(Request request1) {
        if (request1.requestType == RequestType.DEBIT) {
            this.amount = this.amount - request1.amount;

        } else if (request1.requestType == RequestType.CREDIT) {
            this.amount = this.amount + request1.amount;
        }
    }

    public CompletableFuture<Response> credit(int amount) {
        return queue.submit(new Request(amount, RequestType.CREDIT));
    }

    public CompletableFuture<Response> processValidationResponse(Request request) {
        return queue.submit(request);
    }
}
