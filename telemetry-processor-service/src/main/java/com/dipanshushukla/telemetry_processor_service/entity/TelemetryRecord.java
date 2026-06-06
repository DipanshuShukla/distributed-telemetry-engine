package com.dipanshushukla.telemetry_processor_service.entity;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "telemetry_logs")
@Data
public class TelemetryRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String machineId;
    private String scenario;
    private double temperature;
    private double fluidVelocity;
    private double magneticFlux;
    private double powerDraw;
    private double pressure;
    private long timestamp;

    // AI/Rules Inference Output Fields
    private boolean anomalyDetected;
    private String anomalyType; // e.g., "THERMAL_OVERHEAT", "PRESSURE_INSTABILITY"
}