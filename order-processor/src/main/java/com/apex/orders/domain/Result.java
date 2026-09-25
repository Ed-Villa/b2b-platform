package com.apex.orders.domain;

import java.util.List;

public record Result(String status,
                     Client client,
                     List<Line> lines,
                     Totals totals,
                     String reason) {
    public static Result failure(String status, Client client, String reason) {
        return new Result(
                status,
                client,
                List.of(),
                null,
                reason
        );
    }
}