package com.opsmesh.gateway.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;

@Component
public class DatabaseInitializer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DatabaseInitializer.class);
    private final DatabaseClient databaseClient;

    public DatabaseInitializer(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    @Override
    public void run(String... args) {
        log.info("Initializing Phase 3 PostgreSQL relational schema...");

        databaseClient.sql("CREATE EXTENSION IF NOT EXISTS vector;")
                .then()
                .then(databaseClient.sql(
                        "CREATE TABLE IF NOT EXISTS telemetry_warehouse (" +
                                "    id BIGSERIAL PRIMARY KEY," +
                                "    trace_id VARCHAR(64) UNIQUE NOT NULL," +
                                "    service_name VARCHAR(128) NOT NULL," +
                                "    environment VARCHAR(32) NOT NULL," +
                                "    log_level VARCHAR(16) NOT NULL," +
                                "    message TEXT NOT NULL," +
                                "    event_timestamp BIGINT NOT NULL," +
                                "    metadata_json JSONB," +
                                "    embedding vector(1536)" +
                                ");"
                ).then())
                .then(databaseClient.sql(
                        "CREATE INDEX IF NOT EXISTS telemetry_embedding_hnsw_idx " +
                                "ON telemetry_warehouse USING hnsw (embedding vector_cosine_ops);"
                ).then())
                .doOnSuccess(v -> log.info("PostgreSQL storage fabric and vector extensions verified successfully."))
                .doOnError(e -> log.error("Failed to automatically build relational target engine layout", e))
                .subscribe(); // Securely anchors onto the reactive event-loop context on app startup
    }
}