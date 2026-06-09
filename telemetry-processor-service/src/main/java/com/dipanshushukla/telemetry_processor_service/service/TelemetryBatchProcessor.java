package com.dipanshushukla.telemetry_processor_service.service;

import com.dipanshushukla.telemetry_processor_service.entity.TelemetryRecord;
import com.dipanshushukla.telemetry_processor_service.repository.TelemetryRepository;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@AllArgsConstructor
@Slf4j
public class TelemetryBatchProcessor {

    private final TelemetryRepository repository;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    @KafkaListener(topics = "raw-telemetry", groupId = "industrial-processor-group")
    public void processIncomingBatch(List<ConsumerRecord<String, String>> kafkaRecords) {
        List<TelemetryRecord> entitiesToSave = new ArrayList<>();
        long currentWindowSec = Instant.now().getEpochSecond();

        for (ConsumerRecord<String, String> record : kafkaRecords) {
            try {
                TelemetryRecord telemetry = objectMapper.readValue(record.value(), TelemetryRecord.class);
                evaluateAnomalies(telemetry);
                entitiesToSave.add(telemetry);

                // Track active industrial machine assets using a Redis Hash
                redisTemplate.opsForHash().put("fleet:active-machines",
                        telemetry.getMachineId(),
                        String.valueOf(Instant.now().toEpochMilli()));
            } catch (Exception e) {
                log.error("Error parsing payload record: " + e.getMessage());
            }
        }

        // Batch insert into PostgreSQL via a single trip over the network
        if (!entitiesToSave.isEmpty()) {
            repository.saveAll(entitiesToSave);
            log.info("Successfully processed and committed a batch of " + entitiesToSave.size() + " records.");

            // ─── DISTRIBUTED THROUGHPUT METRIC CORE ───
            long nowMs = Instant.now().toEpochMilli();
            int batchSize = entitiesToSave.size();

            // Log this exact batch into a Redis Sorted Set (ZSET)
            // Score = current timestamp, Value = unique_string:batch_size
            redisTemplate.opsForZSet().add(
                    "metrics:throughput:zset",
                    nowMs + ":" + batchSize,
                    (double) nowMs);

            // Keep the ZSET clean by removing data older than 30 seconds
            long longAgo = nowMs - 30000;
            redisTemplate.opsForZSet().removeRangeByScore("metrics:throughput:zset", 0, longAgo);
        }
    }

    /**
     * Mock AI Engine mapping our 3 specific operational scenarios
     */
    private void evaluateAnomalies(TelemetryRecord record) {
        // Use standard thresholds relative to our baseline.csv metrics
        switch (record.getScenario()) {
            case "rayleigh_benard": // Thermal Management Proxy
                if (record.getTemperature() > 355.0) {
                    record.setAnomalyDetected(true);
                    record.setAnomalyType("THERMAL_OVERHEAT_WARNING");
                }
                break;

            case "MHD_64": // Energy Efficiency Proxy
                if (record.getPowerDraw() > 1220.0) {
                    record.setAnomalyDetected(true);
                    record.setAnomalyType("POWER_SURGE_DETECTED");
                }
                break;

            case "rayleigh_taylor": // Safety Regulation Proxy
                if (record.getPressure() > 153.0) {
                    record.setAnomalyDetected(true);
                    record.setAnomalyType("CRITICAL_PRESSURE_INSTABILITY");
                }
                break;
        }
    }
}