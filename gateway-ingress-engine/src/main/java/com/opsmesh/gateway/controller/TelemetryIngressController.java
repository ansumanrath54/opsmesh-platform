package com.opsmesh.gateway.controller;

import com.opsmesh.gateway.config.KafkaProducerService;
import com.opsmesh.gateway.model.TelemetryLogPayload;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/telemetry")
public class TelemetryIngressController {

    private final KafkaProducerService kafkaProducerService;
    private final ReactiveStringRedisTemplate redisTemplate;

    // Redis key namespace prefix to prevent collision spaces
    private static final String IDEMPOTENCY_KEY_PREFIX = "opsmesh:telemetry:lock:";

    public TelemetryIngressController(KafkaProducerService kafkaProducerService,
                                      ReactiveStringRedisTemplate redisTemplate) {
        this.kafkaProducerService = kafkaProducerService;
        this.redisTemplate = redisTemplate;
    }

    @PostMapping("/ingress")
    public Mono<ResponseEntity<Map<String, Object>>> processIncomingTelemetry(
            @RequestBody TelemetryLogPayload payload) {

        String cacheKey = IDEMPOTENCY_KEY_PREFIX + payload.getTraceId();

        return redisTemplate.opsForValue()
                .setIfAbsent(cacheKey, "PENDING", Duration.ofMinutes(5))
                .flatMap(isUnique -> {

                    // FIX: Force Map.<String, Object>of(...) to break the type invariance variance loop
                    if (!isUnique) {
                        return Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(Map.<String, Object>of(
                                        "status", "DUPLICATE_REJECTED",
                                        "traceId", payload.getTraceId(),
                                        "message", "Telemetry frame dropped. Event signature already processed within the rolling safety window."
                                )));
                    }

                    return kafkaProducerService.streamTelemetryEvent(payload)
                            .map(traceId -> ResponseEntity.accepted().body(Map.<String, Object>of(
                                    "status", "ACCEPTED",
                                    "traceId", traceId,
                                    "timestamp", System.currentTimeMillis()
                            )))
                            .onErrorResume(error -> redisTemplate.delete(cacheKey)
                                    .then(Mono.error(error)));
                })
                .defaultIfEmpty(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
    }
}