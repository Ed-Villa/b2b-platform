package com.apex.orders;

import com.apex.orders.domain.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RulesTest {
    static String eventJson(String event, long orderVersion) throws Exception {
        var json = com.apex.orders.adapters.Json.mapper();
        var dto = json.valueToTree(order(event, orderVersion));
        ((com.fasterxml.jackson.databind.node.ObjectNode) dto).put("eventVersion", 1);
        return json.writeValueAsString(dto);
    }

    static Order order(String event, long version) {
        return new Order(event, version, Instant.parse("2026-09-18T15:42:10Z"), "ORD-1", "MX", "MXN", "CLI-99821", "C1", List.of(new Item("PRD-001", 24, new BigDecimal("35.50"))));
    }

    static Client client() {
        return new Client("CLI-99821", "Client", "ACTIVE", "WHOLESALE", "GENERAL", "MX");
    }

    static Product product() {
        return new Product("PRD-001", "Drink", "SKU-1", "ACTIVE", "STANDARD");
    }

    @ParameterizedTest
    @CsvSource({"MX,MXN,STANDARD,16.00", "MX,MXN,REDUCED,8.00", "MX,MXN,EXEMPT,0.00", "CO,COP,STANDARD,19.00", "CO,COP,REDUCED,5.00", "CO,COP,EXEMPT,0.00", "PE,PEN,STANDARD,18.00", "PE,PEN,REDUCED,10.00", "PE,PEN,EXEMPT,0.00"})
    void taxes(String market, String currency, String category, String tax) {
        Order o = new Order("E", 1, Instant.now(), "O", market, currency, "C", null, List.of(new Item("P", 1, new BigDecimal("100"))));
        Result r = new Rules().calculate(o, new Client("C", "C", "ACTIVE", "RETAIL", "GENERAL", market), List.of(new Product("P", "P", "S", "ACTIVE", category)));
        assertEquals(new BigDecimal(tax), r.totals().tax());
    }

    @Test
    void wholesaleAndExempt() {
        Result result = new Rules().calculate(order("E", 1), client(), List.of(product()));
        assertEquals(new BigDecimal("25.56"), result.totals().discount());
        assertEquals(new BigDecimal("132.23"), result.totals().tax());
        assertEquals(new BigDecimal("958.67"), result.totals().grandTotal());
        Client exempt = new Client("CLI-99821", "C", "ACTIVE", "WHOLESALE", "EXEMPT", "MX");
        assertEquals(new BigDecimal("0.00"), new Rules().calculate(order("E", 1), exempt, List.of(product())).totals().tax());
    }

    @ParameterizedTest
    @CsvSource({"19,0.00", "20,0.60"})
    void discountBoundary(int quantity, String expected) {
        Order o = new Order("E", 1, Instant.now(), "O", "MX", "MXN", "C", null, List.of(new Item("PRD-001", quantity, BigDecimal.ONE)));
        assertEquals(new BigDecimal(expected), new Rules().calculate(o, client(), List.of(product())).totals().discount());
    }

    @Test
    void roundingBeforeAggregation() {
        Order o = new Order("E", 1, Instant.now(), "O", "MX", "MXN", "C", null, List.of(new Item("P1", 1, new BigDecimal("0.025")), new Item("P2", 1, new BigDecimal("0.025"))));
        Result r = new Rules().calculate(o, client(), List.of(new Product("P1", "P", "S", "ACTIVE", "STANDARD"), new Product("P2", "P", "S", "ACTIVE", "STANDARD")));
        assertEquals(new BigDecimal("0.06"), r.totals().grossSubtotal());
        assertEquals(new BigDecimal("0.00"), r.totals().tax());
    }

    @Test
    void rejectsIneligibleWithoutPartialTotals() {
        Rules rules = new Rules();
        assertEquals("CLIENT_NOT_FOUND", rules.calculate(order("E", 1), null, List.of()).reason());
        assertEquals("CLIENT_BLOCKED", rules.calculate(order("E", 1), new Client("C", "C", "BLOCKED", "RETAIL", "GENERAL", "MX"), List.of()).reason());
        assertEquals("CLIENT_MARKET_MISMATCH", rules.calculate(order("E", 1), new Client("C", "C", "ACTIVE", "RETAIL", "GENERAL", "CO"), List.of()).reason());
        assertNull(rules.calculate(order("E", 1), client(), List.of()).totals());
        assertTrue(rules.calculate(order("E", 1), client(), List.of(new Product("PRD-001", "P", "S", "DISCONTINUED", "STANDARD"))).reason().startsWith("PRODUCT_DISCONTINUED"));
    }

    @Test
    void rejectsInvalidInputs() {
        for (var items : List.of(List.<Item>of(), List.of(new Item("P", 0, BigDecimal.ONE)), List.of(new Item("P", 1, new BigDecimal("-1"))), List.of(new Item("P", 1, BigDecimal.ONE), new Item("P", 1, BigDecimal.ONE)))) {
            assertThrows(IllegalArgumentException.class, () -> new Rules().validate(new Order("E", 1, Instant.now(), "O", "MX", "MXN", "C", null, items)));
        }
        assertThrows(IllegalArgumentException.class, () -> new Rules().validate(order("E", 0)));
        assertThrows(IllegalArgumentException.class, () -> new Rules().validate(new Order("E", 1, Instant.now(), "O", "CO", "MXN", "C", null, order("E", 1).items())));
    }
}