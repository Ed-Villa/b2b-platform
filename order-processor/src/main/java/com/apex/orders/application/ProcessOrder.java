package com.apex.orders.application;

import com.apex.orders.domain.*;

import java.util.List;

import static com.apex.orders.application.Ports.*;

public final class ProcessOrder {
    private final Catalog catalog;
    private final Store store;
    private final Rules rules;
    private final ConcurrentProducts products;

    public ProcessOrder(Catalog catalog, Store store, Rules rules) {
        this(catalog, store, rules, 20);
    }

    public ProcessOrder(Catalog catalog, Store store, Rules rules, int productConcurrency) {
        this.products = new ConcurrentProducts(catalog, productConcurrency);
        this.catalog = catalog;
        this.store = store;
        this.rules = rules;
    }

    public void process(Envelope envelope, Order order) {
        rules.validate(order);

        if (store.seen(order.eventId())) return;

        Client client = null;
        Result result;
        int attempts = 1;

        try {
            client = catalog.client(order);
            List<Product> enriched = List.of();

            // Reject ineligible clients without calling every product provider.
            if ("ACTIVE".equals(client.status()) && order.market().equals(client.market()))
                enriched = products.fetch(order);
            result = rules.calculate(order, client, enriched);
        } catch (ExternalFailure failure) {
            attempts = failure.attempts;
            result = Result.failure(failure.missing ? "REJECTED" : "TECHNICAL_FAILURE", client, failure.getMessage());
        }
        store.save(envelope, order, result, attempts);
    }
}
