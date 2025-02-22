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
  private Map<String, Map<String, Integer>> detailsOfDataHealthCheckCompletedMap = new HashMap<>();

  public void fetchMixpanelData() {
    try {
      String url = mixpanelApiUrl + "?project_id=" + projectId + "&from_date=2025-02-15&to_date=2025-02-22";

      String auth = username + ":" + secret;
      String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());

      HttpHeaders headers = new HttpHeaders();
      headers.set("Authorization", "Basic " + encodedAuth);
      headers.set("Accept", "application/json");

      HttpEntity<String> entity = new HttpEntity<>(headers);

      ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);

      String responseBody = response.getBody();
      if (responseBody == null || responseBody.isEmpty()) {
        System.out.println("[INFO] No data received from Mixpanel");
        return;
      }

      String[] jsonLines = responseBody.split("\n");
      ObjectMapper objectMapper = new ObjectMapper();
      System.out.println("[Mixpanel API Response]: \n");

      fetchPipelineFailures(jsonLines, objectMapper);

      fetchDataHealthFailures(jsonLines, objectMapper);

      printPipelineFailures();

      printDataHealthCheckFailures();

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

    if (errorMessage.contains("no mails matching the filters") || errorMessage.contains("null for mail report") || errorMessage.contains("no attachment found for mail") || errorMessage.contains("no download link found") || errorMessage.contains("lab_email_pipeline")) {
      return false;
    }

    return true;
  }

  private boolean filterForDataHealthFailureJson(JsonNode propertiesJson) {
    String instance = propertiesJson.has("instance") ? (propertiesJson.get("instance").asText()).toLowerCase() : "";

    if (instance.contains("prod")) {
      return true;
    }

    return false;
  }

  private void printDataHealthCheckFailures() {
    try {
      int countOfDataHealthCheckCompletedFailures = 0;

      System.out.println("Details of data health check failures: ");

      for (String datasetIdKey : detailsOfDataHealthCheckCompletedMap.keySet()) {
        for (String datasetNameKey : detailsOfDataHealthCheckCompletedMap.get(datasetIdKey).keySet()) {
          System.out.println("Dataset ID: " + datasetIdKey + " Dataset Name: " + datasetNameKey + " Failure Count: " + detailsOfDataHealthCheckCompletedMap.get(datasetIdKey).get(datasetNameKey));

          countOfDataHealthCheckCompletedFailures += detailsOfDataHealthCheckCompletedMap.get(datasetIdKey).get(datasetNameKey);
        }
      }

      System.out.println("Total Dataset Health Failure Count: " + countOfDataHealthCheckCompletedFailures);
    } catch (Exception e) {
      System.out.println("[ERROR]");
      System.out.println(e.getStackTrace());
    }
  }

  private void printPipelineFailures() {
    try {
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
    } catch (Exception e) {
      System.out.println("[ERROR]");
      System.out.println(e.getStackTrace());
    }
  }

  private void fetchDataHealthFailures(String[] jsonLines, ObjectMapper objectMapper) {
    try {
      for (String jsonLine : jsonLines) {
        if (jsonLine.trim().isEmpty()) {
          continue;
        }
        JsonNode eventJson = objectMapper.readTree(jsonLine);
        if (
                eventJson.has("event")
                        &&
                        "BE_Health_Check_Completed".equals(eventJson.get("event").asText())
        ) {

          if (!eventJson.has("properties")) {
            System.out.println("[WARN] A BE_Health_Check_Completed event caught without properties JSON");
            continue;
          }
          JsonNode propertiesJson = eventJson.get("properties");

          if (propertiesJson != null && propertiesJson.has("result")
                  && "failed".equals((propertiesJson.get("result").asText()).toLowerCase())
                  && filterForDataHealthFailureJson(propertiesJson)) {

            if (!propertiesJson.has("datasetId")) {
              System.out.println("[WARN] A BE_Health_Check_Completed event caught without a dataset ID");
              continue;
            }
            String datasetIdFromJson = propertiesJson.get("datasetId").asText();

            detailsOfDataHealthCheckCompletedMap.putIfAbsent(datasetIdFromJson, new HashMap<>());

            Map<String, Integer> detailOfDatasetIdFailure = detailsOfDataHealthCheckCompletedMap.get(datasetIdFromJson);

            if (!propertiesJson.has("datasetName")) {
              System.out.println("[WARN] A dataset caught which has dataset ID as: " + datasetIdFromJson + " without having a dataset Name");
              continue;
            }
            String datasetNameFromJson = propertiesJson.get("datasetName").asText();

            detailOfDatasetIdFailure.put(datasetNameFromJson, detailOfDatasetIdFailure.getOrDefault(datasetNameFromJson, 0) + 1);
          }
        }
      }
    } catch (Exception e) {
      System.err.println("[Error] Failed to fetch data from Mixpanel: " + e.getMessage());
      System.out.println(e.getStackTrace());
    }
  }

  private void fetchPipelineFailures(String[] jsonLines, ObjectMapper objectMapper) {
    try {
      for (String jsonLine : jsonLines) {
        if (jsonLine.trim().isEmpty()) {
          continue;
        }
        JsonNode eventJson = objectMapper.readTree(jsonLine);
        if (
                eventJson.has("event")
                        &&
                "BE_pipeline_executed".equals(eventJson.get("event").asText())
        ) {

          if (!eventJson.has("properties")) {
            System.out.println("[WARN] A BE_pipeline_executed event caught without properties JSON");
            continue;
          }
          JsonNode propertiesJson = eventJson.get("properties");

          if (propertiesJson != null && propertiesJson.has("Result")
                  && "fail".equals((propertiesJson.get("Result").asText()).toLowerCase())
                  && filterForPipelineFailureJson(propertiesJson)) {

            if (!propertiesJson.has("Pipeline ID")) {
              System.out.println("[WARN] A BE_pipeline_executed event caught without a pipeline ID");
              continue;
            }
            String pipelineIdFromJson = propertiesJson.get("Pipeline ID").asText();

            detailsOfPipelineFailuresMap.putIfAbsent(pipelineIdFromJson, new HashMap<>());

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
      }
    } catch (Exception e) {
      System.err.println("[Error] Failed to fetch data from Mixpanel: " + e.getMessage());
      System.out.println(e.getStackTrace());
    }
  }
}
