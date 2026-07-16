package com.opsmesh.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TelemetryLogPayload implements Serializable {
    private String traceId;
    private String serviceName;
    private String environment;
    private String logLevel;
    private String message;
    private long timestamp;
    private Map<String, Object> metadata;
}