package com.mediaiq.mixpanel_slack_alert_service.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class SlackNotificationService {
  @Value("${slack.webhook.url}")
  private String slackWebhookUrl;

  private final RestTemplate restTemplate = new RestTemplate();

  public void sendMessageToSlack(String message) {
    try {
      Map<String, String> payload = new HashMap<>();
      payload.put("text", message);

      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_JSON);
      HttpEntity<Map<String, String>> entity = new HttpEntity<>(payload, headers);

      ResponseEntity<String> response = restTemplate.exchange(slackWebhookUrl, HttpMethod.POST, entity, String.class);

      if (response.getStatusCode().is2xxSuccessful()) {
        System.out.println("[SLACK] Message sent Successfully");
      } else {
        System.out.println("[SLACK] Failed to send message" + response.getBody());
      }
    } catch (Exception e) {
      System.err.println("[ERROR] Failed to send Slack notification" + e.getMessage());
    }
  }
}
