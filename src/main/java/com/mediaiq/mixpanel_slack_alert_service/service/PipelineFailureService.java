package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mediaiq.mixpanel_slack_alert_service.Util.BlockUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class PipelineFailureService {
  private final Map<String, Map<String, Integer>> detailsOfPipelineFailuresMap = new HashMap<>();
  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger logger = LoggerFactory.getLogger(PipelineFailureService.class);

  public void fetchPipelineFailures(String[] jsonLines) {
    try {
      detailsOfPipelineFailuresMap.clear();
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
            logger.warn("[WARN] A BE_pipeline_executed event caught without properties JSON");
            continue;
          }
          JsonNode propertiesJson = eventJson.get("properties");

          if (propertiesJson != null && propertiesJson.has("Result")
                  && "fail".equalsIgnoreCase((propertiesJson.get("Result").asText()))
                  && filterForPipelineFailureJson(propertiesJson)) {

            if (!propertiesJson.has("Pipeline ID")) {
              logger.warn("[WARN] A BE_pipeline_executed event caught without a pipeline ID");
              continue;
            }
            String pipelineIdFromJson = propertiesJson.get("Pipeline ID").asText();

            detailsOfPipelineFailuresMap.putIfAbsent(pipelineIdFromJson, new HashMap<>());

            Map<String, Integer> detailsOfPipelineIdFailure = detailsOfPipelineFailuresMap.get(pipelineIdFromJson);

            if (!propertiesJson.has("Pipeline Name")) {
              logger.warn("[WARN] A pipeline caught which has pipeline ID as: {} without having a pipeline Name", pipelineIdFromJson);
              continue;
            }
            String pipelineNameFromJson = propertiesJson.get("Pipeline Name").asText();

            detailsOfPipelineIdFailure.put(pipelineNameFromJson,
                    detailsOfPipelineIdFailure.getOrDefault(pipelineNameFromJson, 0) + 1);
          }
        }
      }
    } catch (Exception e) {
      logger.error("[ERROR] Failed to fetch data from Mixpanel: {}", e.getMessage());
      System.out.println(Arrays.toString(e.getStackTrace()));
    }
  }

  private boolean filterForPipelineFailureJson(JsonNode propertiesJson) {
    String pipelineName = propertiesJson.has("Pipeline Name") ? (propertiesJson.get("Pipeline Name").asText()).toLowerCase() : "";
    String errorMessage = propertiesJson.has("Error Message") ? (propertiesJson.get("Error Message").asText()).toLowerCase() : "";

    if (pipelineName.isEmpty() || pipelineName.contains("test") || pipelineName.contains("backfill") || pipelineName.contains("it_")) {
      return false;
    }

    return !errorMessage.contains("no mails matching the filters") && !errorMessage.contains("null for mail report") && !errorMessage.contains("no attachment found for mail") && !errorMessage.contains("no download link found") && !errorMessage.contains("lab_email_pipeline");
  }

  public void populatePipelineFailures(BlockUtil blockUtil) {
    try {
      int countOfFailedPipelines = 0;

      blockUtil.addBlock(Map.of(
              "type", "header",
              "text", Map.of("type", "plain_text", "text", "🚨 Pipeline Failures", "emoji", true)
      ));

      for (String pipelineId : detailsOfPipelineFailuresMap.keySet()) {
        for (String pipelineName : detailsOfPipelineFailuresMap.get(pipelineId).keySet()) {
          int failureCount = detailsOfPipelineFailuresMap.get(pipelineId).get(pipelineName);
          countOfFailedPipelines += failureCount;

          blockUtil.addBlock(Map.of(
                  "type", "section",
                  "fields", List.of(
                          Map.of("type", "mrkdwn", "text", "*Pipeline ID:* `" + pipelineId + "`"),
                          Map.of("type", "mrkdwn", "text", "*Pipeline Name:* `" + pipelineName + "`"),
                          Map.of("type", "mrkdwn", "text", "*Failure Count:* `" + failureCount + "`")
                  )
          ));

          blockUtil.addBlock(Map.of("type", "divider"));
        }
      }

      if (countOfFailedPipelines == 0) {
        blockUtil.addBlock(Map.of(
                "type", "section",
                "text", Map.of("type", "mrkdwn", "text", "✅ *All good so far!* No pipeline failures detected.")
        ));
      } else if (countOfFailedPipelines > 30) {
        blockUtil.reset();
        blockUtil.addBlock(Map.of(
                "type", "section",
                "text", Map.of("type", "mrkdwn", "text", "🚨 *More than 30 pipelines failed!* Too many failures to display individually.")
        ));
      }
    } catch (Exception e) {
      logger.error("[ERROR] Error while populating pipeline failures");
    }
  }

  public void printPipelineFailures() {
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
      System.out.println(Arrays.toString(e.getStackTrace()));
    }
  }

}
