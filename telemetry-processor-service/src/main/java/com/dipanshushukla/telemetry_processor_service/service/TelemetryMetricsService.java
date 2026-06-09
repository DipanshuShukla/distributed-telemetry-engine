package com.dipanshushukla.telemetry_processor_service.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import com.dipanshushukla.telemetry_processor_service.entity.TelemetryRecord;
import com.dipanshushukla.telemetry_processor_service.repository.TelemetryRepository;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@AllArgsConstructor
public class TelemetryMetricsService {

    private final StringRedisTemplate redisTemplate;
    private final TelemetryRepository repository;

    public Map<String, Object> getClusterMetrics() {
        long nowMs = Instant.now().toEpochMilli();
        long threeSecondsAgo = nowMs - 3000; // 3-second window

        // 1. Fetch only the items that fall within our active 3-second window
        Set<String> rollingSamples = redisTemplate.opsForZSet().rangeByScore(
                "metrics:throughput:zset",
                threeSecondsAgo,
                nowMs);

        long totalEventsInWindow = 0;
        if (rollingSamples != null) {
            for (String sample : rollingSamples) {
                // Parse out the batch size from the "timestamp:batchSize" string format
                String[] parts = sample.split(":");
                if (parts.length == 2) {
                    totalEventsInWindow += Long.parseLong(parts[1]);
                }
            }
        }

        // Calculate exact average events per second across the 3-second span
        long preciseThroughputPerSecond = totalEventsInWindow / 3;

        // 2. Fetch active machine assets
        Set<Object> activeMachines = redisTemplate.opsForHash().keys("fleet:active-machines");

        return Map.of(
                "eventsPerSecond", preciseThroughputPerSecond,
                "activeAssetCount", activeMachines.size(),
                "monitoredAssets", activeMachines);
    }

    public List<TelemetryRecord> getRecentAnomalies() {
        return repository.findTop50ByAnomalyDetectedTrueOrderByTimestampDesc();
    }
}