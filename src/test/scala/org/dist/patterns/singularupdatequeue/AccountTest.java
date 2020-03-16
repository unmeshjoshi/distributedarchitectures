package org.dist.patterns.singularupdatequeue;

import org.dist.patterns.common.RequestOrResponse;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class AccountTest {

    @Test
    public void shouldCreditAfterValidationWithExternalServiceCall() throws ExecutionException, InterruptedException {
        Account account = new Account();
        CompletableFuture<Response> credit = account.credit(100);
        CompletableFuture<Response> responseCompletableFuture = credit.thenCompose(response -> {
            List<RequestOrResponse> values = makeServiceCall(response);
            return account.handleValidationResponse(new Request(0, RequestType.VALIDATION_RESPONSE, values.get(0).getCorrelationId()));
        });

        Response response = responseCompletableFuture.get();
        assertEquals(response.getAmount(), 100);

    }

    private List<RequestOrResponse> makeServiceCall(Response response) {
        //make service call;
        return response.messages.values().stream().collect(Collectors.toList());
    }

}