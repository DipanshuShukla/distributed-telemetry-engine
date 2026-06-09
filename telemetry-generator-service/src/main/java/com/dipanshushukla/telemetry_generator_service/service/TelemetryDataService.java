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

        // 0.2% chance to trigger an anomaly (approx. 100 out of every 1000 events)
        boolean isAnomalySpike = random.nextDouble() < 0.050;

        return new TelemetryEvent(
                base[0], // machineId
                base[1], // scenario
                applyJitter(Double.parseDouble(base[2]), isAnomalySpike), // temp
                applyJitter(Double.parseDouble(base[3]), isAnomalySpike), // velocity
                applyJitter(Double.parseDouble(base[4]), isAnomalySpike), // flux
                applyJitter(Double.parseDouble(base[5]), isAnomalySpike), // power
                applyJitter(Double.parseDouble(base[6]), isAnomalySpike), // pressure
                Instant.now().toEpochMilli());
    }

    private double applyJitter(double baseValue, boolean isAnomalySpike) {
        if (isAnomalySpike) {
            // Anomaly: Spike the value by 5% to 8% so it triggers our mock AI rules
            double spikeMultiplier = 1.05 + (0.03 * random.nextDouble());
            return Math.round((baseValue * spikeMultiplier) * 100.0) / 100.0;
        } else {
            // Normal Operation: Very tight variance of +/- 1% to stay safe
            double normalMultiplier = 0.99 + (0.02 * random.nextDouble());
            return Math.round((baseValue * normalMultiplier) * 100.0) / 100.0;
        }
    }
}