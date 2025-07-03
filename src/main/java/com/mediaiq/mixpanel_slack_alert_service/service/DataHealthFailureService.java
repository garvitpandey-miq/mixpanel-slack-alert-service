package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mediaiq.mixpanel_slack_alert_service.Util.BlockUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DataHealthFailureService {
  private final Map<String, Map<String, Integer>> detailsOfDataHealthCheckCompletedMap = new HashMap<>();
  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger logger = LoggerFactory.getLogger(DataHealthFailureService.class);

  public void fetchDataHealthFailures(String[] jsonLines) {
    try {
      detailsOfDataHealthCheckCompletedMap.clear();
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
            logger.warn("[WARN] A BE_Health_Check_Completed event caught without properties JSON");
            continue;
          }
          JsonNode propertiesJson = eventJson.get("properties");

          if (propertiesJson != null && propertiesJson.has("result")
                  && "failed".equalsIgnoreCase((propertiesJson.get("result").asText()))
                  && filterForDataHealthFailureJson(propertiesJson)) {

            if (!propertiesJson.has("datasetId")) {
              logger.warn("[WARN] A BE_Health_Check_Completed event caught without a dataset ID");
              continue;
            }
            String datasetIdFromJson = propertiesJson.get("datasetId").asText();

            detailsOfDataHealthCheckCompletedMap.putIfAbsent(datasetIdFromJson, new HashMap<>());

            Map<String, Integer> detailOfDatasetIdFailure = detailsOfDataHealthCheckCompletedMap.get(datasetIdFromJson);

            if (!propertiesJson.has("datasetName")) {
              logger.warn("[WARN] A dataset caught which has dataset ID as: {} without having a dataset Name", datasetIdFromJson);
              continue;
            }
            String datasetNameFromJson = propertiesJson.get("datasetName").asText();

            detailOfDatasetIdFailure.put(datasetNameFromJson, detailOfDatasetIdFailure.getOrDefault(datasetNameFromJson, 0) + 1);
          }
        }
      }
    } catch (Exception e) {
      logger.error("[Error] Failed to fetch data from Mixpanel: {}", e.getMessage());
      System.out.println(Arrays.toString(e.getStackTrace()));
    }
  }

  public void populateDataHealthFailures(BlockUtil blockUtil) {
    try {
      int countOfFailedDataHealthChecks = 0;

      blockUtil.addBlock(Map.of(
              "type", "header",
              "text", Map.of("type", "plain_text", "text", "📊 Data Health Check Failures", "emoji", true)
      ));

      for (String datasetId : detailsOfDataHealthCheckCompletedMap.keySet()) {
        for (String datasetName : detailsOfDataHealthCheckCompletedMap.get(datasetId).keySet()) {
          int failureCount = detailsOfDataHealthCheckCompletedMap.get(datasetId).get(datasetName);
          countOfFailedDataHealthChecks += failureCount;

          blockUtil.addBlock(Map.of(
                  "type", "section",
                  "fields", List.of(
                          Map.of("type", "mrkdwn", "text", "*Dataset ID:* `" + datasetId + "`"),
                          Map.of("type", "mrkdwn", "text", "*Dataset Name:* `" + datasetName + "`"),
                          Map.of("type", "mrkdwn", "text", "*Failure Count:* `" + failureCount + "`")
                  )
          ));

          blockUtil.addBlock(Map.of("type", "divider"));
        }
      }

      if (countOfFailedDataHealthChecks == 0) {
        blockUtil.addBlock(Map.of(
                "type", "section",
                "text", Map.of("type", "mrkdwn", "text", "✅ *All good so far!* No data health check failures detected.")
        ));
      } else if (countOfFailedDataHealthChecks > 25) {
        blockUtil.reset();
        blockUtil.addBlock(Map.of(
                "type", "section",
                "text", Map.of("type", "mrkdwn", "text", "📊 *More than 25 data health check failures!* Too many failures to display individually.")
        ));
      }
    } catch (Exception e) {
      logger.error("[ERROR] Error while populating data health failures");
    }
  }

  private boolean filterForDataHealthFailureJson(JsonNode propertiesJson) {
    String instance = propertiesJson.has("instance") ? (propertiesJson.get("instance").asText()).toLowerCase() : "";

    return instance.contains("prod") || instance.isEmpty();
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

