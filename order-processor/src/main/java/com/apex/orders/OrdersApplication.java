package com.apex.orders;

import com.apex.orders.adapters.*;
import com.apex.orders.adapters.mongo.MongoStore;
import com.apex.orders.application.ProcessOrder;
import com.apex.orders.domain.Rules;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;

@SpringBootApplication
@EnableScheduling
public class OrdersApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrdersApplication.class, args);
    }

    @Bean
    ObjectMapper mapper() {
        return Json.mapper();
    }

    @Bean
    MongoStore store(MongoClient client, ObjectMapper json, @Value("${spring.data.mongodb.database}") String database) {
        return new MongoStore(client, database, json);
    }

    @Bean
    HttpCatalog catalog(ObjectMapper json, @Value("${providers.clients}") String clients, @Value("${providers.products}") String products,
                        @Value("${providers.connect-ms:1000}") long connect, @Value("${providers.response-ms:2000}") long response,
                        @Value("${providers.backoff-ms:200}") long backoff, @Value("${providers.retry-after-max-ms:2000}") long retryAfter) {
        return new HttpCatalog(json, clients, products, Duration.ofMillis(connect), Duration.ofMillis(response), backoff, retryAfter);
    }

    @Bean
    ProcessOrder processor(HttpCatalog catalog,
                           MongoStore store,
                           @Value("${providers.products-concurrency:20}") int productConcurrency) {
        return new ProcessOrder(catalog, store, new Rules(), productConcurrency);
    }

    @Bean
    OrderListener listener(ObjectMapper json, ProcessOrder processor, MongoStore store) {
        return new OrderListener(json, processor, store);
    }

    @Bean
    OutboxPublisher publisher(MongoStore store, KafkaTemplate<String, String> kafka,
                              @Value("${outbox.max-permanent-failures:3}") int maxFailures,
                              @Value("${outbox.retry-delay-ms:5000}") long retryDelay) {
        return new OutboxPublisher(store, OutboxPublisher.kafka(kafka), maxFailures, retryDelay);
    }

    @Bean
    DefaultErrorHandler errorHandler() {
        // All parse/validation and external failures are handled before this boundary.
        // Never recover/discard a storage failure or an unexpected exception.
        var handler = new DefaultErrorHandler(new FixedBackOff(2000, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(java.util.Map.of(Exception.class, true), true);
        handler.setAckAfterHandle(false);
        return handler;
    }
}
