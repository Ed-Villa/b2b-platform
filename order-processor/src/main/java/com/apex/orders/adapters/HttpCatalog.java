package com.apex.orders.adapters;

import com.apex.orders.application.Ports.Catalog;
import com.apex.orders.application.Ports.ExternalFailure;
import com.apex.orders.domain.Client;
import com.apex.orders.domain.Order;
import com.apex.orders.domain.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public final class HttpCatalog implements Catalog {
    private final HttpClient http;
    private final ObjectMapper json;
    private final String clients, products;
    private final Duration timeout;
    private final long baseBackoff, retryAfterMax;

    public HttpCatalog(ObjectMapper json,
                       String clients,
                       String products,
                       Duration connect,
                       Duration timeout,
                       long baseBackoff,
                       long retryAfterMax) {
        this.json = json;
        this.clients = clients;
        this.products = products;
        this.timeout = timeout;
        this.baseBackoff = baseBackoff;
        this.retryAfterMax = retryAfterMax;
        http = HttpClient.newBuilder().connectTimeout(connect).build();
    }

    private static boolean member(String value, String... values) {
        return value != null && Set.of(values).contains(value);
    }

    public Client client(Order o) {
        ClientDto c = get(clients + "/clients/" + o.clientId(), o, ClientDto.class, "CLIENT");
        if (!o.clientId().equals(c.clientId())
                || c.name() == null
                || !member(c.status(), "ACTIVE", "BLOCKED")
                || !member(c.segment(), "WHOLESALE", "RETAIL")
                || !member(c.taxRegime(), "GENERAL", "SIMPLIFIED", "EXEMPT")
                || !member(c.market(), "MX", "CO", "PE")
        ) throw new ExternalFailure("CLIENT_INVALID_RESPONSE", false, 1);
        return c.toDomain();
    }

    public Product product(Order o, String id) {
        ProductDto p = get(products + "/products/" + id + "?market=" + o.market(), o, ProductDto.class, "PRODUCT");
        if (!id.equals(p.productId())
                || p.name() == null
                || p.sku() == null
                || !member(p.status(), "ACTIVE", "DISCONTINUED")
                || !member(p.taxCategory(), "STANDARD", "REDUCED", "EXEMPT")
        ) throw new ExternalFailure("PRODUCT_INVALID_RESPONSE", false, 1);
        return p.toDomain();
    }

    private <T> T get(String url, Order order, Class<T> type, String source) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            long wait = baseBackoff * (1L << (attempt - 1));
            String reason;
            try {
                var request = HttpRequest.newBuilder(URI.create(url)).timeout(timeout)
                        .header("X-Order-Id", order.orderId()).header("X-Event-Id", order.eventId())
                        .header("X-Correlation-Id", order.eventId()).GET().build();
                var response = http.send(request, HttpResponse.BodyHandlers.ofString());
                int status = response.statusCode();

                if (status == 200) {
                    try {
                        T body = json.readValue(response.body(), type);
                        if (body == null) throw new IOException("null response");
                        return body;
                    } catch (IOException e) {
                        throw new ExternalFailure(source + "_INVALID_RESPONSE", false, attempt);
                    }
                }

                if (status == 404) throw new ExternalFailure(source + "_NOT_FOUND", true, attempt);

                if (!Set.of(429, 500, 502, 503).contains(status))
                    throw new ExternalFailure(source + "_HTTP_" + status, false, attempt);

                reason = source + "_HTTP_" + status;
                wait = Math.max(wait, retryAfter(response.headers().firstValue("Retry-After").orElse("0")));
            } catch (IOException e) {
                reason = source + "_IO_TIMEOUT";
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("HTTP interrupted; do not commit", e);
            }

            if (attempt == 3) throw new ExternalFailure(reason, false, attempt);

            LoggerFactory.getLogger(HttpCatalog.class).info(
                    "transition=retry orderId={} eventId={} source={} attempt={}",
                    order.orderId(),
                    order.eventId(),
                    source,
                    attempt
            );

            try {
                Thread.sleep(wait + (baseBackoff == 0 ? 0 : ThreadLocalRandom.current().nextLong(51)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Retry interrupted", e);
            }
        }
        throw new IllegalStateException("unreachable");
    }

    private long retryAfter(String value) {
        try {
            return Math.clamp(Long.parseLong(value) * 1000, 0, retryAfterMax);
        } catch (NumberFormatException e) {
            try {
                return Math.clamp(Duration.between(
                        Instant.now(),
                        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
                ).toMillis(), 0, retryAfterMax);
            } catch (RuntimeException ignored) {
                return 0;
            }
        }
    }

    private record ClientDto(String clientId,
                             String name,
                             String status,
                             String segment,
                             String taxRegime,
                             String market) {
        Client toDomain() {
            return new Client(
                    clientId,
                    name,
                    status,
                    segment,
                    taxRegime,
                    market
            );
        }
    }

    private record ProductDto(String productId,
                              String name,
                              String sku,
                              String status,
                              String taxCategory) {
        Product toDomain() {
            return new Product(
                    productId,
                    name,
                    sku,
                    status,
                    taxCategory
            );
        }
    }
}
