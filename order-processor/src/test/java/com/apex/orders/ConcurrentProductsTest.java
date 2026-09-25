package com.apex.orders;

import com.apex.orders.application.*;
import com.apex.orders.application.Ports.*;
import com.apex.orders.domain.*;
import com.apex.orders.domain.Rules;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.IntStream;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Timeout(15)
class ConcurrentProductsTest {
    static Order order(int count) {
        return new Order("E", 1, Instant.now(), "O", "MX", "MXN", "C", null,
            IntStream.range(0, count).mapToObj(i -> new Item("P" + i, 1, BigDecimal.ONE)).toList());
    }
    static Product product(String id) { return new Product(id, id, id, "ACTIVE", "STANDARD"); }
    static Envelope envelope() { return new Envelope("raw", "input", 0, 1, Instant.now()); }
    static void await(CountDownLatch latch) {
        try { assertTrue(latch.await(5, TimeUnit.SECONDS), "Concurrent task did not arrive"); }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException(ex); }
    }

    @Test void usesVirtualThreadsAndPreservesInputOrder() throws Exception {
        Catalog catalog = mock(Catalog.class);
        var both = new CountDownLatch(2);
        var secondFinished = new CountDownLatch(1);
        when(catalog.product(any(), any())).thenAnswer(call -> {
            assertTrue(Thread.currentThread().isVirtual());
            String id = call.getArgument(1);
            both.countDown(); await(both);
            if (id.equals("P0")) await(secondFinished); else secondFinished.countDown();
            return product(id);
        });
        assertEquals(List.of("P0", "P1"), new ConcurrentProducts(catalog, 2).fetch(order(2))
            .stream().map(Product::productId).toList());
    }

    @Test void semaphoreIsSharedAcrossOrdersAndReleasesEveryPermit() throws Exception {
        Catalog catalog = mock(Catalog.class);
        var active = new AtomicInteger(); var peak = new AtomicInteger();
        var saturated = new CountDownLatch(2); var release = new CountDownLatch(1);
        when(catalog.product(any(), any())).thenAnswer(call -> {
            int current = active.incrementAndGet(); peak.accumulateAndGet(current, Math::max);
            try { saturated.countDown(); await(release); return product(call.getArgument(1)); }
            finally { active.decrementAndGet(); }
        });
        var lookup = new ConcurrentProducts(catalog, 2);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var a = pool.submit(() -> lookup.fetch(order(10)));
            var b = pool.submit(() -> lookup.fetch(order(10)));
            try { await(saturated); assertEquals(2, active.get()); }
            finally { release.countDown(); }
            assertEquals(10, a.get(5, TimeUnit.SECONDS).size());
            assertEquals(10, b.get(5, TimeUnit.SECONDS).size());
        }
        assertEquals(2, peak.get()); assertEquals(0, active.get());
        assertEquals(2, lookup.fetch(order(2)).size());
    }

    @Test void failureCancelsSiblingAndPreservesReasonAndAttempts() {
        Catalog catalog = mock(Catalog.class); Store store = mock(Store.class);
        when(catalog.client(any())).thenReturn(RulesTest.client());
        var siblingStarted = new CountDownLatch(1); var siblingStopped = new AtomicBoolean();
        when(catalog.product(any(), eq("P0"))).thenAnswer(call -> {
            await(siblingStarted); throw new ExternalFailure("PRODUCT_HTTP_503", false, 3);
        });
        when(catalog.product(any(), eq("P1"))).thenAnswer(call -> {
            siblingStarted.countDown();
            try { new CountDownLatch(1).await(); return product("P1"); }
            finally { siblingStopped.set(true); }
        });
        var processor = new ProcessOrder(catalog, store, new Rules(), 2);
        processor.process(envelope(), order(2));
        assertTrue(siblingStopped.get(), "Cancelled work must stop before persisting");
        verify(store).save(any(), any(), argThat(result -> result.status().equals("TECHNICAL_FAILURE")
            && result.totals() == null && result.reason().equals("PRODUCT_HTTP_503")), eq(3));
        // Reuse the same processor to detect leaked permits after cancellation/failure.
        reset(catalog); when(catalog.client(any())).thenReturn(RulesTest.client());
        when(catalog.product(any(), any())).thenAnswer(call -> product(call.getArgument(1)));
        processor.process(envelope(), order(2));
        verify(store).save(any(), any(), argThat(result -> result.status().equals("APPROVED")), eq(1));
    }

    @Test void interruptionCancelsRunningAndWaitingTasksWithoutPersistence() throws Exception {
        Catalog catalog = mock(Catalog.class); Store store = mock(Store.class);
        when(catalog.client(any())).thenReturn(RulesTest.client());
        var started = new CountDownLatch(1); var stopped = new AtomicBoolean();
        when(catalog.product(any(), any())).thenAnswer(call -> {
            started.countDown();
            try { new CountDownLatch(1).await(); return product("P0"); }
            finally { stopped.set(true); }
        });
        var processor = new ProcessOrder(catalog, store, new Rules(), 1);
        var failure = new AtomicReference<Throwable>(); var interrupted = new AtomicBoolean();
        Thread caller = Thread.ofPlatform().start(() -> {
            try { processor.process(envelope(), order(10)); }
            catch (Throwable ex) { failure.set(ex); interrupted.set(Thread.currentThread().isInterrupted()); }
        });
        try { await(started); }
        finally { caller.interrupt(); caller.join(5000); }
        assertFalse(caller.isAlive()); assertTrue(stopped.get()); assertTrue(interrupted.get());
        assertInstanceOf(IllegalStateException.class, failure.get());
        verify(store, never()).save(any(), any(), any(), anyInt());
        reset(catalog); when(catalog.client(any())).thenReturn(RulesTest.client());
        when(catalog.product(any(), any())).thenAnswer(call -> product(call.getArgument(1)));
        processor.process(envelope(), order(2));
        verify(store).save(any(), any(), argThat(result -> result.status().equals("APPROVED")), eq(1));
    }

    @ParameterizedTest @CsvSource({"BLOCKED,MX", "ACTIVE,CO"})
    void ineligibleClientNeverStartsProductCalls(String status, String market) {
        Catalog catalog = mock(Catalog.class); Store store = mock(Store.class);
        when(catalog.client(any())).thenReturn(new Client("C", "C", status, "RETAIL", "GENERAL", market));
        new ProcessOrder(catalog, store, new Rules(), 20).process(envelope(), order(3));
        verify(catalog, times(1)).client(any()); verify(catalog, never()).product(any(), any());
        verify(store).save(any(), any(), argThat(result -> result.status().equals("REJECTED")), eq(1));
    }

    @Test void validClientIsFetchedOnceBeforeProducts() {
        Catalog catalog = mock(Catalog.class); Store store = mock(Store.class);
        var validated = new AtomicBoolean();
        when(catalog.client(any())).thenAnswer(call -> { validated.set(true); return RulesTest.client(); });
        when(catalog.product(any(), any())).thenAnswer(call -> {
            assertTrue(validated.get()); return product(call.getArgument(1));
        });
        new ProcessOrder(catalog, store, new Rules(), 2).process(envelope(), order(3));
        verify(catalog, times(1)).client(any()); verify(catalog, times(3)).product(any(), any());
        verify(store).save(any(), any(), argThat(result -> result.status().equals("APPROVED")
            && result.lines().stream().map(Line::productId).toList().equals(List.of("P0", "P1", "P2"))), eq(1));
    }

    @Test void missingProductIsRejectedWithoutPartialTotals() {
        Catalog catalog = mock(Catalog.class); Store store = mock(Store.class);
        when(catalog.client(any())).thenReturn(RulesTest.client());
        when(catalog.product(any(), any())).thenThrow(new ExternalFailure("PRODUCT_NOT_FOUND", true, 1));
        new ProcessOrder(catalog, store, new Rules(), 2).process(envelope(), order(2));
        verify(store).save(any(), any(), argThat(result -> result.status().equals("REJECTED")
            && result.totals() == null), eq(1));
    }

    @Test void invalidLimitsFailFast() {
        assertThrows(IllegalArgumentException.class, () -> new ConcurrentProducts(mock(Catalog.class), 0));
        assertThrows(IllegalArgumentException.class, () -> new ConcurrentProducts(mock(Catalog.class), -1));
    }
}
