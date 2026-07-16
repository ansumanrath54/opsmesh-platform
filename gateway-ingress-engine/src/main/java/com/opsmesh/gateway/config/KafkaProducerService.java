package com.opsmesh.gateway.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opsmesh.gateway.model.TelemetryLogPayload;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    // 1. Change the template value type to String to use the default out-of-the-box serializer
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private static final String TOPIC = "opsmesh-syslog-transit";

    // 2. Inject Jackson's ObjectMapper alongside the template
    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public Mono<String> streamTelemetryEvent(TelemetryLogPayload payload) {
        return Mono.create(sink -> {
            try {
                // 3. Serialize the payload object into a clean JSON string
                String jsonPayload = objectMapper.writeValueAsString(payload);

                kafkaTemplate.send(TOPIC, payload.getTraceId(), jsonPayload)
                        .whenComplete((result, error) -> {
                            if (error != null) {
                                log.error("Kafka push failed for trace [" + payload.getTraceId() + "]", error);
                                sink.error(error);
                            } else {
                                log.info("Pushed trace [" + payload.getTraceId() + "] to Kafka partition [" + result.getRecordMetadata().partition() + "]");
                                sink.success(payload.getTraceId());
                            }
                        });
            } catch (JsonProcessingException e) {
                log.error("JSON Serialization failed for trace [" + payload.getTraceId() + "]", e);
                sink.error(e);
            }
        });
    }
}