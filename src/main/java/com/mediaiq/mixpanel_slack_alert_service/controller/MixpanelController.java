package com.mediaiq.mixpanel_slack_alert_service.controller;

import com.mediaiq.mixpanel_slack_alert_service.service.MixpanelAlertManager;
import com.mediaiq.mixpanel_slack_alert_service.service.SlackNotificationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/mixpanel-slack")
public class MixpanelController {

  private final MixpanelAlertManager mixpanelAlertManager;

  public MixpanelController(MixpanelAlertManager mixpanelAlertManager, SlackNotificationService slackNotificationService) {
    this.mixpanelAlertManager = mixpanelAlertManager;
  }

  @GetMapping("/alert")
  public String testSlackNotification() {
    mixpanelAlertManager.fetchAndSendSlackNotification();
    return "Slack message sent!";
  }

}
