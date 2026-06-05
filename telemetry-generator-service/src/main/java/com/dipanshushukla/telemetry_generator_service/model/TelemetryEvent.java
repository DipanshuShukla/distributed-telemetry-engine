package com.dipanshushukla.telemetry_generator_service.model;

public record TelemetryEvent(
                String machineId,
                String scenario,
                double temperature,
                double fluidVelocity,
                double magneticFlux,
                double powerDraw,
                double pressure,
                long timestamp) {
}