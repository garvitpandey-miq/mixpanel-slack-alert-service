package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;
import java.util.Base64;
import java.util.HashMap;
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

  private Map<String, Map<String, Integer>> detailsOfPipelineFailuresMap = new HashMap<>();

  public void fetchMixpanelData() {
    try {
      String url = mixpanelApiUrl + "?project_id=" + projectId + "&from_date=2025-02-19&to_date=2025-02-19";

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

            if (eventJson.has("properties")) {
              JsonNode propertiesJson = eventJson.get("properties");
              if (propertiesJson != null && propertiesJson.has("Result")
                      && "Fail".equals(propertiesJson.get("Result").asText())
                      && filterForPipelineFailureJson(propertiesJson)) {

                if (!propertiesJson.has("Pipeline ID")) {
                  System.out.println("[WARN] A BE_pipeline_executed event caught without a pipeline ID");
                  continue;
                }
                String pipelineIdFromJson = propertiesJson.get("Pipeline ID").asText();

                if (!detailsOfPipelineFailuresMap.containsKey(pipelineIdFromJson)) {
                  detailsOfPipelineFailuresMap.put(pipelineIdFromJson, new HashMap<>());
                }

                Map<String, Integer> detailsOfPipelineIdFailure = detailsOfPipelineFailuresMap.get(pipelineIdFromJson);

                if (!propertiesJson.has("Pipeline Name")) {
                  System.out.println("[WARN] A pipeline caught which has pipeline ID as: " + pipelineIdFromJson + " without having a pipeline Name");
                  continue;
                }
                String pipelineNameFromJson = propertiesJson.get("Pipeline Name").asText();

                detailsOfPipelineIdFailure.put(pipelineNameFromJson,
                        detailsOfPipelineIdFailure.getOrDefault(pipelineNameFromJson, 0) + 1);

              }
            }

//            String formattedJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(eventJson);
//            System.out.println(formattedJson);
          }

//          if (
//                  eventJson.has("event")
//                          &&
//                          "BE_Health_Check_Completed".equals(eventJson.get("event").asText())
//          ) {
//            String formattedJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(eventJson);
//            System.out.println(formattedJson);
//          }
        }
      }

      int countOfFailedPipelines = 0;

      System.out.println("Details of failed pipelines: ");

      for (String pipelineIdKey : detailsOfPipelineFailuresMap.keySet()) {
        for (String pipelineNameKey : detailsOfPipelineFailuresMap.get(pipelineIdKey).keySet()) {
          System.out.println("Pipeline ID: " + pipelineIdKey + " Pipeline Name: " + pipelineNameKey + " Failure Count: "
                  + detailsOfPipelineFailuresMap.get(pipelineIdKey).get(pipelineNameKey));
          countOfFailedPipelines += detailsOfPipelineFailuresMap.get(pipelineIdKey).get(pipelineNameKey);
        }
      }

      System.out.println("Total pipeline failed count: " + countOfFailedPipelines);

    }
    catch (Exception e) {
      System.err.println("[Error] Failed to fetch data from Mixpanel: " + e.getMessage());
      System.out.println(e.getStackTrace());
    }
  }

  private boolean filterForPipelineFailureJson(JsonNode propertiesJson) {
    String pipelineName = propertiesJson.has("Pipeline Name") ? (propertiesJson.get("Pipeline Name").asText()).toLowerCase() : "";
    String errorMessage = propertiesJson.has("Error Message") ? (propertiesJson.get("Error Message").asText()).toLowerCase() : "";

    if ("".equals(pipelineName) || pipelineName.contains("test") || pipelineName.contains("backfill") || pipelineName.contains("it_")) {
      return false;
    }

    if (errorMessage.contains("no mails matching the filters") || errorMessage.contains("null for mail report") || errorMessage.contains("No attachment found for mail") || errorMessage.contains("no download link found") || errorMessage.contains("lab_email_pipeline")) {
      return false;
    }

    return true;
  }
}
