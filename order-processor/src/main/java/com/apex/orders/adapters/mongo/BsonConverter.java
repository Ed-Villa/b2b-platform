package com.apex.orders.adapters.mongo;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

class BsonConverter {
    private final ObjectMapper json;

    BsonConverter(ObjectMapper json) {
        this.json = json;
    }

    Document toDocument(Object value) {
        try {
            Object decoded = json.readerFor(Object.class)
                    .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .readValue(json.writeValueAsString(value));
            return (Document) toBson(decoded);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Object toBson(Object value) {
        if (value instanceof BigDecimal decimal) return new Decimal128(decimal);
        if (value instanceof Map<?, ?> map) {
            Document doc = new Document();
            map.forEach((key, item) -> doc.put(key.toString(), toBson(item)));
            return doc;
        }
        if (value instanceof List<?> list) return list.stream().map(this::toBson).toList();
        return value;
    }
}
