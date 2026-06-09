package com.dipanshushukla.telemetry_processor_service.service;

import com.dipanshushukla.telemetry_processor_service.entity.TelemetryRecord;
import com.dipanshushukla.telemetry_processor_service.repository.TelemetryRepository;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
@Slf4j
public class TelemetryBatchProcessor {

    private final TelemetryRepository repository;
    private final ObjectMapper objectMapper;
    private final TelemetryCacheService cacheService; // Injecting our new service!

    @KafkaListener(topics = "raw-telemetry", groupId = "industrial-processor-group")
    public void processIncomingBatch(List<ConsumerRecord<String, String>> kafkaRecords) {
        List<TelemetryRecord> entitiesToSave = new ArrayList<>();
        long nowMs = Instant.now().toEpochMilli();

        // Variables to hold the absolute most important data point for the live chart
        Double priorityThermal = null;
        Double priorityEnergy = null;
        Double prioritySafety = null;

        // Flags to lock the snapshot if an anomaly is found in this batch
        boolean thermalAnomalyFound = false;
        boolean energyAnomalyFound = false;
        boolean safetyAnomalyFound = false;

        for (ConsumerRecord<String, String> record : kafkaRecords) {
            try {
                TelemetryRecord telemetry = objectMapper.readValue(record.value(), TelemetryRecord.class);
                evaluateAnomalies(telemetry);
                entitiesToSave.add(telemetry);

                // Update fleet heartbeat
                cacheService.recordMachineHeartbeat(telemetry.getMachineId(), nowMs);

                // ─── ANOMALY-FIRST SNAPSHOT AGGREGATION ───
                if ("rayleigh_benard".equals(telemetry.getScenario())) {
                    if (telemetry.isAnomalyDetected()) {
                        priorityThermal = telemetry.getTemperature();
                        thermalAnomalyFound = true; // Lock it in
                    } else if (!thermalAnomalyFound) {
                        priorityThermal = telemetry.getTemperature(); // Overwrite with latest normal
                    }
                } else if ("MHD_64".equals(telemetry.getScenario())) {
                    if (telemetry.isAnomalyDetected()) {
                        priorityEnergy = telemetry.getPowerDraw();
                        energyAnomalyFound = true;
                    } else if (!energyAnomalyFound) {
                        priorityEnergy = telemetry.getPowerDraw();
                    }
                } else if ("rayleigh_taylor".equals(telemetry.getScenario())) {
                    if (telemetry.isAnomalyDetected()) {
                        prioritySafety = telemetry.getPressure();
                        safetyAnomalyFound = true;
                    } else if (!safetyAnomalyFound) {
                        prioritySafety = telemetry.getPressure();
                    }
                }

            } catch (Exception e) {
                log.error("Error parsing payload record: " + e.getMessage());
            }
        }

        // 1. Commit to PostgreSQL
        if (!entitiesToSave.isEmpty()) {
            repository.saveAll(entitiesToSave);
            log.info("Successfully processed and committed a batch of " + entitiesToSave.size() + " records.");

            // 2. Commit throughput metrics to Redis
            cacheService.recordThroughput(entitiesToSave.size(), nowMs);

            // 3. Push our highly-optimized, anomaly-aware snapshots to the dashboard
            cacheService.pushLiveSnapshots(priorityThermal, priorityEnergy, prioritySafety);
        }
    }

    /**
     * Mock AI Engine with Stateful Debouncing (Requires 3 consecutive breaches)
     */
    private void evaluateAnomalies(TelemetryRecord record) {
        int requiredBreaches = 3; // Must breach 3 times in a row to trigger alarm

        switch (record.getScenario()) {
            case "rayleigh_benard": // Thermal Management Proxy
                if (record.getTemperature() > 355.0) {
                    boolean isSustained = cacheService.registerAndCheckBreach(record.getMachineId(), "thermal",
                            requiredBreaches);
                    if (isSustained) {
                        record.setAnomalyDetected(true);
                        record.setAnomalyType("THERMAL_OVERHEAT_WARNING");
                    }
                } else {
                    // Safe reading! Reset the consecutive counter to zero.
                    cacheService.clearBreachState(record.getMachineId(), "thermal");
                }
                break;

            case "MHD_64": // Energy Efficiency Proxy
                if (record.getPowerDraw() > 1220.0) {
                    boolean isSustained = cacheService.registerAndCheckBreach(record.getMachineId(), "energy",
                            requiredBreaches);
                    if (isSustained) {
                        record.setAnomalyDetected(true);
                        record.setAnomalyType("POWER_SURGE_DETECTED");
                    }
                } else {
                    cacheService.clearBreachState(record.getMachineId(), "energy");
                }
                break;

            case "rayleigh_taylor": // Safety Regulation Proxy
                if (record.getPressure() > 153.0) {
                    boolean isSustained = cacheService.registerAndCheckBreach(record.getMachineId(), "safety",
                            requiredBreaches);
                    if (isSustained) {
                        record.setAnomalyDetected(true);
                        record.setAnomalyType("CRITICAL_PRESSURE_INSTABILITY");
                    }
                } else {
                    cacheService.clearBreachState(record.getMachineId(), "safety");
                }
                break;
        }
    }
}