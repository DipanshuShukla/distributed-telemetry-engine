package com.dipanshushukla.telemetry_generator_service.service;

import com.dipanshushukla.telemetry_generator_service.model.TelemetryEvent;
import jakarta.annotation.PostConstruct;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
public class TelemetryDataService {

    private final List<String[]> baselines = new ArrayList<>();
    private final Random random = new Random();

    @PostConstruct
    public void loadBaselines() {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new ClassPathResource("static/baseline.csv").getInputStream()))) {
            String line;
            br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                baselines.add(line.split(","));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load baseline.csv", e);
        }
    }

    public TelemetryEvent generateNextEvent() {
        if (baselines.isEmpty())
            return null;

        String[] base = baselines.get(random.nextInt(baselines.size()));

        return new TelemetryEvent(
                base[0], // machineId
                base[1], // scenario
                applyJitter(Double.parseDouble(base[2])), // temp
                applyJitter(Double.parseDouble(base[3])), // velocity
                applyJitter(Double.parseDouble(base[4])), // flux
                applyJitter(Double.parseDouble(base[5])), // power
                applyJitter(Double.parseDouble(base[6])), // pressure
                Instant.now().toEpochMilli());
    }

    private double applyJitter(double value) {
        double jitterMultiplier = 0.98 + (0.04 * random.nextDouble());
        return Math.round((value * jitterMultiplier) * 100.0) / 100.0;
    }
}