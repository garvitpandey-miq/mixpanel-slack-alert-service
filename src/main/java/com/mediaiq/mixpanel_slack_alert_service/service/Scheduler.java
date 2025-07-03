package com.mediaiq.mixpanel_slack_alert_service.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class Scheduler {

  private final MixpanelAlertManager mixpanelAlertManager;

  public Scheduler(MixpanelAlertManager mixpanelAlertManager) {
    this.mixpanelAlertManager = mixpanelAlertManager;
  }

  @Scheduled(fixedRate = 120000) 
  public void devFetchMixpanelData() {
    System.out.println("[DEV MODE] Sending data to slack every 2 minutes...");
    mixpanelAlertManager.fetchAndSendSlackNotification();

  }

  @Scheduled(cron = "0 0 3,9,15,21 * * ?", zone = "Asia/Kolkata")
  public void prodFetchMixpanelData() {
    System.out.println("[PROD MODE] Fetching Mixpanel data...");
    mixpanelAlertManager.fetchAndSendSlackNotification();
  }
}
