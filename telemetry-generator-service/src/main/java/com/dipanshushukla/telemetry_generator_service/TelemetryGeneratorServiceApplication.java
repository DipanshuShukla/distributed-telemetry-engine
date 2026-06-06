package com.dipanshushukla.telemetry_generator_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import tools.jackson.databind.ObjectMapper;

@SpringBootApplication
public class TelemetryGeneratorServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(TelemetryGeneratorServiceApplication.class, args);
	}

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}

}
