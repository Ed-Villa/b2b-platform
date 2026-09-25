package com.apex.orders.domain;

import java.math.BigDecimal;

public record Line(String productId,
                   String name,
                   String sku,
                   String taxCategory,
                   int quantity,
                   BigDecimal unitPrice,
                   Totals amounts) {
}