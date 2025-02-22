package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@Service
public class MixpanelFetchService {
  @Value("${mixpanel.api.url}")
  private String mixpanelApiUrl;

  @Value("${mixpanel.project.id}")
  private String projectId;

  @Value("${mixpanel.service.username}")
  private String username;

  @Value("${mixpanel.service.secret}")
  private String secret;

  private final RestTemplate restTemplate = new RestTemplate();

  public String fetchMixpanelData() {
    try {
      String url = mixpanelApiUrl + "?project_id=" + projectId + "&from_date=2025-02-15&to_date=2025-02-22";

      String auth = username + ":" + secret;
      String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());

      HttpHeaders headers = new HttpHeaders();
      headers.set("Authorization", "Basic " + encodedAuth);
      headers.set("Accept", "application/json");

      HttpEntity<String> entity = new HttpEntity<>(headers);

      ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);

      return response.getBody();
    }
    catch (Exception e) {
      System.err.println("[Error] Failed to fetch data from Mixpanel: " + e.getMessage());
      System.out.println(e.getStackTrace());
      return null;
    }
  }

}
