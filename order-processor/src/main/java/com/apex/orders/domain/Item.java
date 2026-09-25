package com.apex.orders.domain;

import java.math.BigDecimal;

public record Item(String productId, int quantity, BigDecimal unitPrice) {
}