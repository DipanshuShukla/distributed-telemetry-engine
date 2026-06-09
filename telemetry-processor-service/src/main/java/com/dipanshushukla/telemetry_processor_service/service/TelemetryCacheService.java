package com.dipanshushukla.telemetry_processor_service.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import lombok.AllArgsConstructor;

import java.util.concurrent.TimeUnit;

@Service
@AllArgsConstructor
public class TelemetryCacheService {

    private final StringRedisTemplate redisTemplate;

    // 1. Records the exact batch size for our sliding window throughput math
    public void recordThroughput(int batchSize, long nowMs) {
        redisTemplate.opsForZSet().add("metrics:throughput:zset", nowMs + ":" + batchSize, (double) nowMs);
        long longAgo = nowMs - 30000;
        redisTemplate.opsForZSet().removeRangeByScore("metrics:throughput:zset", 0, longAgo);
    }

    // 2. Updates the machine's last-seen heartbeat
    public void recordMachineHeartbeat(String machineId, long nowMs) {
        redisTemplate.opsForHash().put("fleet:active-machines", machineId, String.valueOf(nowMs));
    }

    // 3. Pushes the priority-filtered snapshots to the live chart
    public void pushLiveSnapshots(Double thermal, Double energy, Double safety) {
        if (thermal != null) {
            redisTemplate.opsForValue().set("live:thermal", String.valueOf(thermal));
        }
        if (energy != null) {
            redisTemplate.opsForValue().set("live:energy", String.valueOf(energy));
        }
        if (safety != null) {
            redisTemplate.opsForValue().set("live:safety", String.valueOf(safety));
        }
    }

    // ─── STATEFUL ANOMALY DEBOUNCING ───

    public boolean registerAndCheckBreach(String machineId, String metricType, int requiredConsecutiveBreaches) {
        String key = "state:breach:" + machineId + ":" + metricType;

        // Atomically increment the consecutive breach count
        Long currentCount = redisTemplate.opsForValue().increment(key);

        // Set a 30-second TTL. If the machine stops sending data, we don't want a stale
        // breach count hanging around forever.
        redisTemplate.expire(key, 30, TimeUnit.SECONDS);

        return currentCount != null && currentCount >= requiredConsecutiveBreaches;
    }

    public void clearBreachState(String machineId, String metricType) {
        String key = "state:breach:" + machineId + ":" + metricType;
        redisTemplate.delete(key);
    }
}