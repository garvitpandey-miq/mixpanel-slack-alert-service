package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;

@Service
@EnableScheduling
public class MixpanelService {
  private final MixpanelFetchService mixpanelFetchService;
  private final PipelineFailureService pipelineFailureService;
  private final DataHealthFailureService dataHealthFailureService;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public MixpanelService(MixpanelFetchService mixpanelFetchService, PipelineFailureService pipelineFailureService, DataHealthFailureService dataHealthFailureService) {
    this.mixpanelFetchService = mixpanelFetchService;
    this.pipelineFailureService = pipelineFailureService;
    this.dataHealthFailureService = dataHealthFailureService;
  }

  public String fetchAndProcessMixpanelData() {
    try {
      String responseBody = mixpanelFetchService.fetchMixpanelData();

      if (responseBody == null || responseBody.isEmpty()) {
        System.out.println("[INFO] No data received from Mixpanel");
        return null;
      }

      String[] jsonLines = responseBody.split("\n");

      return pipelineFailureService.fetchPipelineFailures(jsonLines) + "\n" +
              dataHealthFailureService.fetchDataHealthFailures(jsonLines);
    } catch(Exception e) {
      System.err.println("[ERROR] Failed to fetch and process Mixpanel data: " + e.getMessage());
      return null;
    }

  }
}
