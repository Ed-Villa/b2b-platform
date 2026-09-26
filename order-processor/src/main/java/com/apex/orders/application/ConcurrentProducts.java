package com.apex.orders.application;

import com.apex.orders.domain.Order;
import com.apex.orders.domain.Product;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * One shared bulkhead per processor instance, including HTTP retries and backoff.
 */
public final class ConcurrentProducts {
    private final Ports.Catalog catalog;
    private final Semaphore permits;

    public ConcurrentProducts(Ports.Catalog catalog, int concurrency) {
        if (concurrency < 1) throw new IllegalArgumentException("providers.products-concurrency must be positive");
        this.catalog = catalog;
        this.permits = new Semaphore(concurrency, true);
    }

    public List<Product> fetch(Order order) {
        var futures = new ArrayList<Future<Product>>();

        // Scoped to the order: close joins cancelled tasks before persistence or offset handling.
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                for (var item : order.items())
                    futures.add(executor.submit(() -> {
                        permits.acquire();
                        try {
                            return catalog.product(order, item.productId());
                        } finally {
                            permits.release();
                        }
                    }));
                var products = new ArrayList<Product>();

                // All tasks already running in parallel; just collecting results in submit order (predictable, not "first done").
                for (var future : futures) products.add(future.get());
                return products;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Product enrichment interrupted; do not commit", ex);
            } catch (ExecutionException ex) {
                if (ex.getCause() instanceof RuntimeException failure) throw failure;
                if (ex.getCause() instanceof Error failure) throw failure;
                throw new IllegalStateException("Product enrichment failed; do not commit", ex.getCause());
            } finally {
                // Interrupt task threads without marking their futures complete early.
                // close() must wait for actual task exit, including permit release.
                executor.shutdownNow();
            }
        }
    }
}
