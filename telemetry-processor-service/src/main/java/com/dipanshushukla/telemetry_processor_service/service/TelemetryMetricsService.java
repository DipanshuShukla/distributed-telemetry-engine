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

        // 1. Calculate Throughput
        Set<String> rollingSamples = redisTemplate.opsForZSet().rangeByScore(
                "metrics:throughput:zset",
                threeSecondsAgo,
                nowMs);

        long totalEventsInWindow = 0;
        if (rollingSamples != null) {
            for (String sample : rollingSamples) {
                String[] parts = sample.split(":");
                if (parts.length == 2) {
                    totalEventsInWindow += Long.parseLong(parts[1]);
                }
            }
        }
        long preciseThroughputPerSecond = totalEventsInWindow / 3;

        // 2. Fetch active machine assets and their last seen timestamps
        Map<Object, Object> machinesMap = redisTemplate.opsForHash().entries("fleet:active-machines");

        // 3. Fetch latest live chart snapshots
        String liveThermal = redisTemplate.opsForValue().get("live:thermal");
        String liveEnergy = redisTemplate.opsForValue().get("live:energy");
        String liveSafety = redisTemplate.opsForValue().get("live:safety");

        return Map.of(
                "eventsPerSecond", preciseThroughputPerSecond,
                "monitoredAssets", machinesMap,
                "liveThermal", liveThermal != null ? Double.parseDouble(liveThermal) : 0.0,
                "liveEnergy", liveEnergy != null ? Double.parseDouble(liveEnergy) : 0.0,
                "liveSafety", liveSafety != null ? Double.parseDouble(liveSafety) : 0.0);
    }

    public List<TelemetryRecord> getRecentAnomalies() {
        return repository.findTop50ByAnomalyDetectedTrueOrderByTimestampDesc();
    }
}