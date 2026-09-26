package com.apex.orders;

import com.apex.orders.adapters.HttpCatalog;
import com.apex.orders.adapters.Json;
import com.apex.orders.application.Ports.ExternalFailure;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class HttpCatalogTest {
    HttpServer server;
    AtomicInteger calls;
    HttpCatalog catalog;

    @BeforeEach
    void setup() throws Exception {
        calls = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress(0), 0);
        String url = "http://localhost:" + server.getAddress().getPort();
        catalog = new HttpCatalog(Json.mapper(), url, url, Duration.ofSeconds(1), Duration.ofSeconds(2), 0, 0);
    }

    void endpoint(int failures, int status, String body, long delay) {
        server.createContext("/clients/", exchange -> {
            int call = calls.incrementAndGet();
            assertEquals("E", exchange.getRequestHeaders().getFirst("X-Correlation-Id"));
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            
            try (exchange) {
                exchange.sendResponseHeaders(call <= failures ? status : 200, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void retriesThenSucceeds() throws Exception {
        endpoint(2, 503, Json.mapper().writeValueAsString(RulesTest.client()), 0);
        assertEquals("ACTIVE", catalog.client(RulesTest.order("E", 1)).status());
        assertEquals(3, calls.get());
    }

    @Test
    void retries429() throws Exception {
        endpoint(1, 429, Json.mapper().writeValueAsString(RulesTest.client()), 0);
        catalog.client(RulesTest.order("E", 1));
        assertEquals(2, calls.get());
    }

    @Test
    void exhaustsTransient() {
        endpoint(10, 503, "{}", 0);
        ExternalFailure e = assertThrows(ExternalFailure.class, () -> catalog.client(RulesTest.order("E", 1)));
        assertEquals(3, e.attempts);
        assertFalse(e.missing);
    }

    @Test
    void neverRetries404() {
        endpoint(10, 404, "{}", 0);
        assertTrue(assertThrows(ExternalFailure.class, () -> catalog.client(RulesTest.order("E", 1))).missing);
        assertEquals(1, calls.get());
    }

    @Test
    void neverRetries400() {
        endpoint(10, 400, "{}", 0);
        assertFalse(assertThrows(ExternalFailure.class, () -> catalog.client(RulesTest.order("E", 1))).missing);
        assertEquals(1, calls.get());
    }

    @Test
    void malformedResponseIsDefinitive() {
        endpoint(0, 200, "{}", 0);
        assertThrows(ExternalFailure.class, () -> catalog.client(RulesTest.order("E", 1)));
        assertEquals(1, calls.get());
    }

    @Test
    void timesOut() {
        String url = "http://localhost:" + server.getAddress().getPort();
        catalog = new HttpCatalog(Json.mapper(), url, url, Duration.ofSeconds(1), Duration.ofMillis(100), 0, 0);
        endpoint(0, 200, "{}", 200);
        assertEquals(3, assertThrows(ExternalFailure.class, () -> catalog.client(RulesTest.order("E", 1))).attempts);
    }
}
