package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.Map;

@Service
public class SlackNotificationService {
  @Value("${slack.webhook.url}")
  private String slackWebhookUrl;

  private final RestTemplate restTemplate = new RestTemplate();
  private static final Logger logger = LoggerFactory.getLogger(SlackNotificationService.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public void sendMessageToSlack(Map<String, Object> jsonPayload) {
    try {
      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_JSON);

      String payloadString = objectMapper.writeValueAsString(jsonPayload);

      HttpEntity<String> entity = new HttpEntity<>(payloadString, headers);
      ResponseEntity<String> response = restTemplate.exchange(slackWebhookUrl, HttpMethod.POST, entity, String.class);

      if (!response.getStatusCode().is2xxSuccessful()) {
        logger.error("[ERROR] Failed to send message to slack: {}", response.getBody());
      }
    } catch (Exception e) {
      logger.error("[ERROR] Failed to send message to Slack{}", e.getMessage());
      System.out.println(Arrays.toString(e.getStackTrace()));
    }
  }
}
