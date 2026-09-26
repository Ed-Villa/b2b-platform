package com.apex.orders;

import com.apex.orders.adapters.Json;
import com.apex.orders.adapters.OrderListener;
import com.apex.orders.application.Ports.Catalog;
import com.apex.orders.application.Ports.Store;
import com.apex.orders.application.ProcessOrder;
import com.apex.orders.domain.Rules;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class ListenerTest {
    @Test
    void unsupportedSchemaNeverProcessesOrder() throws Exception {
        Store store = mock(Store.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        String raw = RulesTest.eventJson("E", 2).replace("\"eventVersion\":1", "\"eventVersion\":9");
        new OrderListener(Json.mapper(), null, store).receive(new ConsumerRecord<>("orders.created.v1", 0, 1, "O", raw), ack);
        verify(store).invalid(any(), eq("E"), eq("ORD-1"), eq("UNSUPPORTED_SCHEMA_VERSION"));
        verify(ack).acknowledge();
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "0", "-1", "1.5"})
    void invalidOrderRevisionIsRejected(String value) throws Exception {
        Store store = mock(Store.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        String raw = RulesTest.eventJson("E", 2);
        raw = value.equals("missing") ? raw.replace("\"orderVersion\":2,", "") : raw.replace("\"orderVersion\":2", "\"orderVersion\":" + value);
        new OrderListener(Json.mapper(), null, store).receive(new ConsumerRecord<>("orders.created.v1", 0, 1, "O", raw), ack);
        verify(store).invalid(any(), eq("E"), eq("ORD-1"), eq("INVALID_INPUT"));
    }

    @Test
    void invalidIsStoredBeforeAcknowledging() {
        Store store = mock(Store.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        new OrderListener(Json.mapper(), null, store).receive(new ConsumerRecord<>("orders.created.v1", 0, 1, "O", "{\"eventVersion\":1}"), ack);
        var sequence = inOrder(store, ack);
        sequence.verify(store).invalid(any(), isNull(), isNull(), eq("INVALID_INPUT"));
        sequence.verify(ack).acknowledge();
    }

    @Test
    void storageFailureDoesNotAcknowledge() throws Exception {
        Store store = mock(Store.class);
        Catalog catalog = mock(Catalog.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        when(catalog.client(any())).thenReturn(RulesTest.client());
        when(catalog.product(any(), any())).thenReturn(RulesTest.product());
        doThrow(new IllegalStateException("mongo unavailable")).when(store).save(any(), any(), any(), anyInt());
        var listener = new OrderListener(Json.mapper(), new ProcessOrder(catalog, store, new Rules()), store);
        var record = new ConsumerRecord<>("orders.created.v1", 0, 1, "O", RulesTest.eventJson("E", 1));
        assertThrows(IllegalStateException.class, () -> listener.receive(record, ack));
        verifyNoInteractions(ack);
    }

    @Test
    void fractionalQuantityIsInvalid() throws Exception {
        Store store = mock(Store.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        String raw = RulesTest.eventJson("E", 1).replace("\"quantity\":24", "\"quantity\":1.5");
        new OrderListener(Json.mapper(), null, store).receive(new ConsumerRecord<>("orders.created.v1", 0, 1, "O", raw), ack);
        verify(store).invalid(any(), eq("E"), eq("ORD-1"), eq("INVALID_INPUT"));
    }
}