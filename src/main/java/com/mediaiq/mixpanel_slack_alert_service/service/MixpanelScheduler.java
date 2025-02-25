package com.mediaiq.mixpanel_slack_alert_service.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class MixpanelScheduler {

  private final MixpanelService mixpanelService;

  public MixpanelScheduler(MixpanelService mixpanelService) {
    this.mixpanelService = mixpanelService;
  }

  @Scheduled(fixedRate = 120000) 
  public void devFetchMixpanelData() {
    System.out.println("[DEV MODE] Fetching Mixpanel data...");
    System.out.println(mixpanelService.fetchAndProcessMixpanelData());
  }

  @Scheduled(cron = "0 0 3,9,15,21 * * ?", zone = "Asia/Kolkata")
  public void prodFetchMixpanelData() {
    System.out.println("[PROD MODE] Fetching Mixpanel data...");
    mixpanelService.fetchAndProcessMixpanelData();
  }
}
