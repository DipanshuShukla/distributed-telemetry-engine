package com.dipanshushukla.telemetry_processor_service.repository;

import com.dipanshushukla.telemetry_processor_service.entity.TelemetryRecord;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TelemetryRepository extends JpaRepository<TelemetryRecord, Long> {

    // Grabs the 50 most recent anomalies to display on the dashboard
    List<TelemetryRecord> findTop50ByAnomalyDetectedTrueOrderByTimestampDesc();
}