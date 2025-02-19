package com.mediaiq.mixpanel_slack_alert_service.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;
import java.util.Base64;

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

      System.out.println("Mixpanel API Response: " + response.getBody());

    } catch (Exception e) {
      System.err.println("Error fetching data from Mixpanel: " + e.getMessage());
    }
  }
}
