package com.apex.orders.domain;

import java.time.Instant;
import java.util.List;

/**
 * Complete order snapshot. orderVersion is its business revision, not the event schema version.
 */
public record Order(String eventId,
                    long orderVersion,
                    Instant occurredAt,
                    String orderId,
                    String market,
                    String currency,
                    String clientId,
                    String channel,
                    List<Item> items) {
}