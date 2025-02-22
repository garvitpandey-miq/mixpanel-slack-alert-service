package com.mediaiq.mixpanel_slack_alert_service.controller;

import com.mediaiq.mixpanel_slack_alert_service.service.MixpanelService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/mixpanel")
public class MixpanelController {

  private final MixpanelService mixpanelService;

  public MixpanelController(MixpanelService mixpanelService) {
    this.mixpanelService = mixpanelService;
  }

  @GetMapping("/fetch")
  public String fetchData() {
    mixpanelService.fetchAndProcessMixpanelData();
    return "Mixpanel data fetch triggered!";
  }

}
