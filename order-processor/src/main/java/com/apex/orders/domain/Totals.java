package com.apex.orders.domain;

import java.math.BigDecimal;

public record Totals(BigDecimal grossSubtotal,
                     BigDecimal discount,
                     BigDecimal netSubtotal,
                     BigDecimal tax,
                     BigDecimal grandTotal) {
}