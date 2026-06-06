package com.dipanshushukla.telemetry_processor_service.service;

import com.dipanshushukla.telemetry_processor_service.entity.TelemetryRecord;
import com.dipanshushukla.telemetry_processor_service.repository.TelemetryRepository;
import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class TelemetryBatchProcessor {

    private final TelemetryRepository repository;
    private final ObjectMapper objectMapper;

    public TelemetryBatchProcessor(TelemetryRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "raw-telemetry", groupId = "industrial-processor-group")
    public void processIncomingBatch(List<ConsumerRecord<String, String>> kafkaRecords) {
        List<TelemetryRecord> entitiesToSave = new ArrayList<>();

        for (ConsumerRecord<String, String> record : kafkaRecords) {
            try {
                // 1. Deserialization~~~~
                TelemetryRecord telemetry = objectMapper.readValue(record.value(), TelemetryRecord.class);

                // 2. Execute Mock AI Model Inference (Rules-Based Engine)
                evaluateAnomalies(telemetry);

                entitiesToSave.add(telemetry);
            } catch (Exception e) {
                System.err.println("Error parsing payload record: " + e.getMessage());
            }
        }

        // 3. Batch insert into PostgreSQL via a single trip over the network
        if (!entitiesToSave.isEmpty()) {
            repository.saveAll(entitiesToSave);
            System.out
                    .println("Successfully processed and committed a batch of " + entitiesToSave.size() + " records.");
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