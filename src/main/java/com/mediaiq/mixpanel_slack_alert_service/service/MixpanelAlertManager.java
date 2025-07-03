package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mediaiq.mixpanel_slack_alert_service.Util.BlockUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;

@Service
@EnableScheduling
public class MixpanelAlertManager {
  private final MixpanelFetchService mixpanelFetchService;
  private final PipelineFailureService pipelineFailureService;
  private final DataHealthFailureService dataHealthFailureService;
  private final SlackNotificationService slackNotificationService;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger logger = LoggerFactory.getLogger(MixpanelAlertManager.class);

  public MixpanelAlertManager(MixpanelFetchService mixpanelFetchService, PipelineFailureService pipelineFailureService,
                              DataHealthFailureService dataHealthFailureService, SlackNotificationService slackNotificationService) {
    this.mixpanelFetchService = mixpanelFetchService;
    this.pipelineFailureService = pipelineFailureService;
    this.dataHealthFailureService = dataHealthFailureService;
    this.slackNotificationService = slackNotificationService;
  }

  public void fetchAndSendSlackNotification() {
    try {
      String responseBody = mixpanelFetchService.fetchMixpanelData();

      if (responseBody == null || responseBody.isEmpty()) {
        logger.info("[INFO] No data received from Mixpanel");
      }

      if (responseBody != null) {
        String[] jsonLines = responseBody.split("\n");
        BlockUtil blockUtil = new BlockUtil();

        pipelineFailureService.fetchPipelineFailures(jsonLines);
        pipelineFailureService.populatePipelineFailures(blockUtil);
        slackNotificationService.sendMessageToSlack(blockUtil.toSlackPayload());

        blockUtil.reset();
        dataHealthFailureService.fetchDataHealthFailures(jsonLines);
        dataHealthFailureService.populateDataHealthFailures(blockUtil);

        blockUtil.addTerminator();
        slackNotificationService.sendMessageToSlack(blockUtil.toSlackPayload());

        blockUtil.reset();
      }

    } catch(Exception e) {
      logger.error("[ERROR] Failed to fetch and process Mixpanel data: {}", e.getMessage());
    }

  }
}
