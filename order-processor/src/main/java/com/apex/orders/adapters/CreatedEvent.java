package com.apex.orders.adapters;

import com.apex.orders.domain.Item;
import com.apex.orders.domain.Order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Wire DTO: eventVersion versions the schema; orderVersion orders business updates.
 */
public record CreatedEvent(String eventId,
                           long eventVersion,
                           long orderVersion,
                           String occurredAt,
                           String orderId,
                           String market,
                           String currency,
                           String clientId,
                           String channel,
                           List<ItemDto> items) {
    public Order toDomain() {
        if (eventVersion != 1) throw new IllegalArgumentException("UNSUPPORTED_SCHEMA_VERSION");
        return new Order(eventId, orderVersion, occurredAt == null ? null : Instant.parse(occurredAt), orderId, market, currency, clientId, channel,
                items == null ? null : items.stream().map(item -> item == null ? null : new Item(item.productId(), item.quantity(), item.unitPrice())).toList());
    }

    public record ItemDto(String productId, int quantity, BigDecimal unitPrice) {
    }
}