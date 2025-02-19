package com.mediaiq.mixpanel_slack_alert_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication

public class MixpanelSlackAlertServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(MixpanelSlackAlertServiceApplication.class, args);
	}

}
