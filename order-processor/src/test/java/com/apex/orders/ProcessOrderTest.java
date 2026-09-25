package com.apex.orders;

import com.apex.orders.application.Ports.Catalog;
import com.apex.orders.application.Ports.Envelope;
import com.apex.orders.application.Ports.ExternalFailure;
import com.apex.orders.application.Ports.Store;
import com.apex.orders.application.ProcessOrder;
import com.apex.orders.domain.Result;
import com.apex.orders.domain.Rules;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

class ProcessOrderTest {
    @Test
    void exhaustedProviderPersistsTechnicalFailureWithAttempts() {
        Catalog catalog = mock(Catalog.class);
        Store store = mock(Store.class);
        when(catalog.client(any())).thenReturn(RulesTest.client());
        when(catalog.product(any(), any())).thenThrow(new ExternalFailure("PRODUCT_HTTP_503", false, 3));
        new ProcessOrder(catalog, store, new Rules()).process(new Envelope("raw", "input", 0, 1, Instant.now()), RulesTest.order("E", 1));
        var result = ArgumentCaptor.forClass(Result.class);
        verify(store).save(any(), any(), result.capture(), eq(3));
        assertEquals("TECHNICAL_FAILURE", result.getValue().status());
        assertNull(result.getValue().totals());
        assertEquals(RulesTest.client(), result.getValue().client());
    }

    @Test
    void missingProviderResourceIsRejected() {
        Catalog catalog = mock(Catalog.class);
        Store store = mock(Store.class);
        when(catalog.client(any())).thenThrow(new ExternalFailure("CLIENT_NOT_FOUND", true, 1));
        new ProcessOrder(catalog, store, new Rules()).process(new Envelope("raw", "input", 0, 1, Instant.now()), RulesTest.order("E", 1));
        var result = ArgumentCaptor.forClass(Result.class);
        verify(store).save(any(), any(), result.capture(), eq(1));
        assertEquals("REJECTED", result.getValue().status());
    }

    @Test
    void knownDuplicateDoesNotCallProviders() {
        Catalog catalog = mock(Catalog.class);
        Store store = mock(Store.class);
        when(store.seen("E")).thenReturn(true);
        new ProcessOrder(catalog, store, new Rules()).process(new Envelope("raw", "input", 0, 1, Instant.now()), RulesTest.order("E", 1));
        verifyNoInteractions(catalog);
        verify(store, never()).save(any(), any(), any(), anyInt());
    }
}