package com.dipanshushukla.telemetry_generator_service.service;

import com.dipanshushukla.telemetry_generator_service.model.TelemetryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@EnableScheduling
public class TelemetryPublisherService {

    private final TelemetryDataService dataService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic}")
    private String topic;

    @Value("${app.telemetry.events-per-tick}")
    private int eventsPerTick;

    public TelemetryPublisherService(TelemetryDataService dataService,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.dataService = dataService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    // Runs 10 times a second
    @Scheduled(fixedRate = 100)
    public void publishEvents() {

        for (int i = 0; i < eventsPerTick; i++) {
            TelemetryEvent event = dataService.generateNextEvent();
            if (event == null)
                continue;

            try {
                String jsonPayload = objectMapper.writeValueAsString(event);
                kafkaTemplate.send(topic, event.machineId(), jsonPayload);
            } catch (JsonProcessingException e) {
                System.err.println("Failed to serialize event: " + e.getMessage());
            }
        }
    }
}