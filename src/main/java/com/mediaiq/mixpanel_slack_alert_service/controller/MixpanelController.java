package com.mediaiq.mixpanel_slack_alert_service.controller;

import com.mediaiq.mixpanel_slack_alert_service.service.MixpanelService;
import com.mediaiq.mixpanel_slack_alert_service.service.SlackNotificationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/mixpanel")
public class MixpanelController {

  private final MixpanelService mixpanelService;
  private final SlackNotificationService slackNotificationService;

  public MixpanelController(MixpanelService mixpanelService, SlackNotificationService slackNotificationService) {
    this.mixpanelService = mixpanelService;
    this.slackNotificationService = slackNotificationService;
  }

  @GetMapping("/fetch")
  public String fetchData() {
//    mixpanelService.fetchAndProcessMixpanelData();
    return "Mixpanel data fetch triggered!";
  }

  @GetMapping("/test")
  public String testSlackNotification() {
    slackNotificationService.sendMessageToSlack(mixpanelService.fetchAndProcessMixpanelData());
    return "Slack message sent!";
  }

}
