package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

@Service
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

  public void fetchAndProcessMixpanelData() {
    try {
      String responseBody = mixpanelFetchService.fetchMixpanelData();

      if (responseBody == null || responseBody.isEmpty()) {
        System.out.println("[INFO] No data received from Mixpanel");
        return;
      }

      String[] jsonLines = responseBody.split("\n");

      pipelineFailureService.fetchPipelineFailures(jsonLines);
      dataHealthFailureService.fetchDataHealthFailures(jsonLines);

      pipelineFailureService.printPipelineFailures();
      dataHealthFailureService.printDataHealthCheckFailures();
    } catch(Exception e) {
      System.err.println("[ERROR] Failed to fetch and process Mixpanel data: " + e.getMessage());
    }

  }
}
