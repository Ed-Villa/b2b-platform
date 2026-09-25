package com.apex.orders.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;


public final class Rules {
    private static final Map<String, String> CURRENCIES = Map.of("MX", "MXN", "CO", "COP", "PE", "PEN");
    private static final Map<String, List<String>> RATES = Map.of(
            "MX", List.of("0.16", "0.08"),
            "CO", List.of("0.19", "0.05"),
            "PE", List.of("0.18", "0.10")
    );

    /**
     * Validates that a string is a well-formed identifier: non-null and matching
     * the pattern of alphanumeric characters, underscores and hyphens, 1 to 128 characters long.
     *
     * @param s the string to validate
     * @return {@code true} if {@code s} is a valid identifier, {@code false} otherwise
     */
    public static boolean id(String s) {
        return s != null && s.matches("[A-Za-z0-9_-]{1,128}");
    }

    /**
     * Rounds a monetary value to 2 decimal places using HALF_UP rounding.
     *
     * @param value the value to round
     * @return the rounded value with a scale of 2
     */
    private static BigDecimal money(BigDecimal value) {
        return value.setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * Validates the structural integrity of an order, including identifiers, version,
     * timestamp, market/currency consistency, and item constraints (count, uniqueness,
     * quantity and price bounds).
     *
     * @param o the order to validate
     * @throws IllegalArgumentException if the order or any of its fields are invalid
     */
    public void validate(Order o) {
        if (o == null || !id(o.eventId()) || !id(o.orderId()) || !id(o.clientId()) || o.orderVersion() < 1 || o.occurredAt() == null)
            throw new IllegalArgumentException("INVALID_IDENTIFIERS_OR_VERSION");
        if (o.market() == null || !CURRENCIES.containsKey(o.market()) || !CURRENCIES.get(o.market()).equals(o.currency()))
            throw new IllegalArgumentException("INVALID_MARKET_CURRENCY");
        if (o.items() == null || o.items().isEmpty() || o.items().size() > 100)
            throw new IllegalArgumentException("INVALID_ITEMS_COUNT");
        Set<String> seen = new HashSet<>();
        for (Item i : o.items()) {
            if (i == null || !id(i.productId()) || !seen.add(i.productId()) || i.quantity() < 1 || i.quantity() > 1_000_000
                    || i.unitPrice() == null || i.unitPrice().signum() < 0 || i.unitPrice().compareTo(new BigDecimal("1000000000")) > 0
                    || i.unitPrice().scale() > 6) throw new IllegalArgumentException("INVALID_ITEM");
        }
    }

    /**
     * Calculates the pricing, discounts, taxes and totals for an order after validating it
     * and checking client and product eligibility.
     *
     * @param o        the order to calculate
     * @param c        the client placing the order, or {@code null} if not found
     * @param products the catalog of products referenced by the order's items
     * @return a {@link Result} indicating either approval with computed line items and totals,
     * or rejection with a failure reason
     * @throws IllegalArgumentException if the order fails structural validation
     */
    public Result calculate(Order o, Client c, List<Product> products) {
        validate(o);

        if (c == null)
            return Result.failure("REJECTED", null, "CLIENT_NOT_FOUND");
        if (!"ACTIVE".equals(c.status()))
            return Result.failure("REJECTED", c, "CLIENT_BLOCKED");
        if (!o.market().equals(c.market()))
            return Result.failure("REJECTED", c, "CLIENT_MARKET_MISMATCH");

        Map<String, Product> byId = new HashMap<>();
        products.forEach(p -> byId.put(p.productId(), p));

        List<Line> lines = new ArrayList<>();
        Totals sum = new Totals(money(BigDecimal.ZERO), money(BigDecimal.ZERO), money(BigDecimal.ZERO), money(BigDecimal.ZERO), money(BigDecimal.ZERO));

        for (Item i : o.items()) {
            Product p = byId.get(i.productId());

            if (p == null)
                return Result.failure("REJECTED", c, "PRODUCT_NOT_FOUND:" + i.productId());
            if (!"ACTIVE".equals(p.status()))
                return Result.failure("REJECTED", c, "PRODUCT_DISCONTINUED:" + i.productId());

            BigDecimal gross = money(i.unitPrice().multiply(BigDecimal.valueOf(i.quantity())));
            BigDecimal discount = money(gross.multiply(new BigDecimal("WHOLESALE".equals(c.segment()) && i.quantity() >= 20 ? "0.03" : "0")));
            BigDecimal net = money(gross.subtract(discount));
            String rate = "EXEMPT".equals(c.taxRegime()) || "EXEMPT".equals(p.taxCategory()) ? "0" : RATES.get(o.market()).get("STANDARD".equals(p.taxCategory()) ? 0 : 1);
            BigDecimal tax = money(net.multiply(new BigDecimal(rate)));
            Totals amounts = new Totals(gross, discount, net, tax, money(net.add(tax)));
            lines.add(new Line(p.productId(), p.name(), p.sku(), p.taxCategory(), i.quantity(), i.unitPrice(), amounts));
            sum = new Totals(sum.grossSubtotal().add(gross), sum.discount().add(discount), sum.netSubtotal().add(net), sum.tax().add(tax), sum.grandTotal().add(amounts.grandTotal()));
        }

        return new Result("APPROVED", c, List.copyOf(lines), sum, null);
    }
}
