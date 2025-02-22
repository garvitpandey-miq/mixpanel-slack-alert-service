package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Service
public class DataHealthFailureService {
  private final Map<String, Map<String, Integer>> detailsOfDataHealthCheckCompletedMap = new HashMap<>();
  private final ObjectMapper objectMapper = new ObjectMapper();

  public void fetchDataHealthFailures(String[] jsonLines) {
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
                  && "failed".equalsIgnoreCase((propertiesJson.get("result").asText()))
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
      System.out.println(Arrays.toString(e.getStackTrace()));
    }
  }

  private boolean filterForDataHealthFailureJson(JsonNode propertiesJson) {
    String instance = propertiesJson.has("instance") ? (propertiesJson.get("instance").asText()).toLowerCase() : "";

    return instance.contains("prod");
  }

  public void printDataHealthCheckFailures() {
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
      System.out.println(Arrays.toString(e.getStackTrace()));
    }
  }

}
