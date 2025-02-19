package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;
import java.util.Base64;
import java.util.Map;

@Service
public class MixpanelService {

  @Value("${mixpanel.api.url}")
  private String mixpanelApiUrl;

  @Value("${mixpanel.project.id}")
  private String projectId;

  @Value("${mixpanel.service.username}")
  private String username;

  @Value("${mixpanel.service.secret}")
  private String secret;

  private final RestTemplate restTemplate = new RestTemplate();

  public void fetchMixpanelData() {
    try {
      String url = mixpanelApiUrl + "?project_id=" + projectId + "&from_date=2025-02-18&to_date=2025-02-18";

      String auth = username + ":" + secret;
      String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());

      HttpHeaders headers = new HttpHeaders();
      headers.set("Authorization", "Basic " + encodedAuth);
      headers.set("Accept", "application/json");

      HttpEntity<String> entity = new HttpEntity<>(headers);

      ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);

//      System.out.println(response.getBody() + "\n");

      String responseBody = response.getBody();
      if (responseBody == null || responseBody.isEmpty()) {
        System.out.println("[INFO] No data received from Mixpanel");
        return;
      }

      String[] jsonLines = responseBody.split("\n");
      ObjectMapper objectMapper = new ObjectMapper();
      System.out.println("[Mixpanel API Response]: \n");

      for (String jsonLine : jsonLines) {
        if (!jsonLine.trim().isEmpty()) {
          JsonNode eventJson = objectMapper.readTree(jsonLine);

          if (
                  eventJson.has("event")
                          &&
                  "BE_pipeline_executed".equals(eventJson.get("event").asText())
          ) {
            String formattedJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(eventJson);
            System.out.println(formattedJson);
          }

          if (
                  eventJson.has("event")
                          &&
                          "BE_Health_Check_Completed".equals(eventJson.get("event").asText())
          ) {
            String formattedJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(eventJson);
            System.out.println(formattedJson);
          }
        }
      }
    } catch (Exception e) {
      System.err.println("[Error] Failed to fetch data from Mixpanel: " + e.getMessage());
    }
  }
}
