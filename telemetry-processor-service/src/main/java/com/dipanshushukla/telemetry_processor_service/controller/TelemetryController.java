package com.dipanshushukla.telemetry_processor_service.controller;

import com.dipanshushukla.telemetry_processor_service.entity.TelemetryRecord;
import com.dipanshushukla.telemetry_processor_service.service.TelemetryMetricsService;
import org.springframework.web.bind.annotation.*;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/telemetry")
@CrossOrigin(origins = "*")
@AllArgsConstructor
public class TelemetryController {

    private final TelemetryMetricsService metricsService;

    @GetMapping("/cluster-metrics")
    public Map<String, Object> getClusterMetrics() {
        return metricsService.getClusterMetrics();
    }

    @GetMapping("/recent-anomalies")
    public List<TelemetryRecord> getRecentAnomalies() {
        return metricsService.getRecentAnomalies();
    }
}