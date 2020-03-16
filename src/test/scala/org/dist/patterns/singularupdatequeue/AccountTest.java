package org.dist.patterns.singularupdatequeue;

import org.dist.kvstore.InetAddressAndPort;
import org.dist.patterns.common.RequestOrResponse;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class AccountTest {

    @Test
    public void shouldCreditAfterValidationWithExternalServiceCall() throws ExecutionException, InterruptedException {
        Account account = new Account();
        CompletableFuture<Response> credit = account.credit(100);
        CompletableFuture<List<Response>> responseCompletableFuture = credit.thenCompose(response -> {
            List<CompletableFuture<RequestOrResponse>> values = makeServiceCall(response);
            List<CompletableFuture<Response>> responseFutures = values.stream().map(future -> {
                return future.thenCompose(r -> account.validationResponse(new Request(0, RequestType.VALIDATION_RESPONSE, r.getCorrelationId())));
            }).collect(Collectors.toList());
            return sequence(responseFutures);
        });

        List<Response> response = responseCompletableFuture.get();
        assertEquals(response.size(), 1);
        assertEquals(response.get(0).getAmount(), 100);
    }

    static<T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> com) {
        return CompletableFuture.allOf(com.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> com.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                );
    }

    private List<CompletableFuture<RequestOrResponse>> makeServiceCall(Response response) {
        List<CompletableFuture<RequestOrResponse>> responses = new ArrayList<>();
        //make service call;
        Map<InetAddressAndPort, RequestOrResponse> messages = response.messages;
        Set<InetAddressAndPort> addresses = messages.keySet();
        for (InetAddressAndPort address : addresses) {
            responses.add(CompletableFuture.completedFuture(messages.get(address)));
        }
        return responses;
    }

}