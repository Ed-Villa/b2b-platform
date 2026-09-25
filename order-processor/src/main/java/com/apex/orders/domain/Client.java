package com.apex.orders.domain;

public record Client(String clientId,
                     String name,
                     String status,
                     String segment,
                     String taxRegime,
                     String market) {
}