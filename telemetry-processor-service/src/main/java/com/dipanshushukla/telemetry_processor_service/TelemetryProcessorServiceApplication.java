package com.dipanshushukla.telemetry_processor_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import tools.jackson.databind.ObjectMapper;

@SpringBootApplication
public class TelemetryProcessorServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(TelemetryProcessorServiceApplication.class, args);
	}

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}

}
