package com.apex.orders.domain;

public record Product(String productId,
                      String name,
                      String sku,
                      String status,
                      String taxCategory) {
}