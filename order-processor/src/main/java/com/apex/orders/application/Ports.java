package com.apex.orders.application;

import com.apex.orders.domain.Client;
import com.apex.orders.domain.Order;
import com.apex.orders.domain.Product;
import com.apex.orders.domain.Result;

import java.time.Instant;

public final class Ports {
    private Ports() {
    }

    public interface Catalog {
        Client client(Order order);

        Product product(Order order, String productId);
    }

    public interface Store {
        boolean seen(String eventId);

        void save(Envelope envelope,
                  Order order,
                  Result result,
                  int attempts);

        void invalid(Envelope envelope,
                     String eventId,
                     String orderId,
                     String reason);
    }

    public record Envelope(String raw,
                           String topic,
                           int partition,
                           long offset,
                           Instant receivedAt) {
        public String receiptId() {
            return topic + ":" + partition + ":" + offset;
        }
    }

    public static class ExternalFailure extends RuntimeException {
        public final boolean missing;
        public final int attempts;

        public ExternalFailure(String reason, boolean missing, int attempts) {
            super(reason);
            this.missing = missing;
            this.attempts = attempts;
        }
    }
}