package com.opsmesh.gateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opsmesh.gateway.model.TelemetryLogPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class TelemetryLogConsumer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryLogConsumer.class);
    private final ObjectMapper objectMapper;
    private final DatabaseClient databaseClient;

    public TelemetryLogConsumer(ObjectMapper objectMapper, DatabaseClient databaseClient) {
        this.objectMapper = objectMapper;
        this.databaseClient = databaseClient;
    }

    // Listens directly onto the transit topic you validated in Phase 1 & 2
    @KafkaListener(topics = "opsmesh-syslog-transit", groupId = "opsmesh-analytics-group")
    public void consumeTelemetryTransitLog(String rawJsonMessage) {
        Mono.defer(() -> {
            try {
                // 1. Map the JSON string back to our stable payload object structure
                TelemetryLogPayload payload = objectMapper.readValue(rawJsonMessage, TelemetryLogPayload.class);
                String metadataJsonString = objectMapper.writeValueAsString(payload.getMetadata());

                // Mocking a placeholder 1536-dimension float array for vector alignment step
                float[] dummyEmbedding = new float[1536];
                dummyEmbedding[0] = 0.015f;

                // FIX: Convert the float[] array into the native text format pgvector expects: "[0.015,0,0,...]"
                StringBuilder vectorStringBuilder = new StringBuilder("[");
                for (int i = 0; i < dummyEmbedding.length; i++) {
                    vectorStringBuilder.append(dummyEmbedding[i]);
                    if (i < dummyEmbedding.length - 1) {
                        vectorStringBuilder.append(",");
                    }
                }
                vectorStringBuilder.append("]");
                String vectorString = vectorStringBuilder.toString();

                log.info("Kafka consumer intercepted telemetry event log for trace: [{}]", payload.getTraceId());

                // 2. Execute non-blocking reactive SQL insert directly into PostgreSQL with manual vector casting
                return databaseClient.sql("INSERT INTO telemetry_warehouse (trace_id, service_name, environment, log_level, message, event_timestamp, metadata_json, embedding) " +
                                "VALUES (:traceId, :serviceName, :environment, :logLevel, :message, :timestamp, :metadataJson::jsonb, :embedding::vector)")
                        .bind("traceId", payload.getTraceId())
                        .bind("serviceName", payload.getServiceName())
                        .bind("environment", payload.getEnvironment())
                        .bind("logLevel", payload.getLogLevel())
                        .bind("message", payload.getMessage())
                        .bind("timestamp", payload.getTimestamp())
                        .bind("metadataJson", metadataJsonString)
                        .bind("embedding", vectorString) // Bound as a string that Postgres will cast internally to a vector
                        .then()
                        .doOnSuccess(v -> log.info("Successfully persisted trace [{}] down into PostgreSQL vector fabric.", payload.getTraceId()));

            } catch (Exception e) {
                log.error("Failed to parse and store incoming Kafka message payload", e);
                return Mono.empty();
            }
        }).subscribe(); // Securely anchors the stream loop execution inside Netty's worker threads
    }
}