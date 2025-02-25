package com.mediaiq.mixpanel_slack_alert_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Service
public class PipelineFailureService {
  private final Map<String, Map<String, Integer>> detailsOfPipelineFailuresMap = new HashMap<>();
  private final ObjectMapper objectMapper = new ObjectMapper();

  public String fetchPipelineFailures(String[] jsonLines) {
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
            System.out.println("[WARN] A BE_pipeline_executed event caught without properties JSON");
            continue;
          }
          JsonNode propertiesJson = eventJson.get("properties");

          if (propertiesJson != null && propertiesJson.has("Result")
                  && "fail".equalsIgnoreCase((propertiesJson.get("Result").asText()))
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

      return mapToString();
    } catch (Exception e) {
      System.err.println("[Error] Failed to fetch data from Mixpanel: " + e.getMessage());
      System.out.println(Arrays.toString(e.getStackTrace()));
      return null;
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

  public String mapToString() {
    try {
      int countOfFailedPipelines = 0;
      StringBuilder pipelineFailureStringBuilder = new StringBuilder();

      pipelineFailureStringBuilder.append("Pipeline Failures: \n");

      for (String pipelineIdKey : detailsOfPipelineFailuresMap.keySet()) {
        for (String pipelineNameKey : detailsOfPipelineFailuresMap.get(pipelineIdKey).keySet()) {
          pipelineFailureStringBuilder.append("Pipeline ID: ").append(pipelineIdKey).append(" Pipeline Name: ").append(pipelineNameKey).append(" Failure Count: ").append(detailsOfPipelineFailuresMap.get(pipelineIdKey).get(pipelineNameKey)).append("\n");
          countOfFailedPipelines += detailsOfPipelineFailuresMap.get(pipelineIdKey).get(pipelineNameKey);
        }
      }

      if (countOfFailedPipelines == 0) {
        return pipelineFailureStringBuilder.append("All good so far!!\n").toString();
      } else if (countOfFailedPipelines > 0 && countOfFailedPipelines < 30) {
        return pipelineFailureStringBuilder.toString();
      } else {
        return """
                Pipeline Failures:\s
                More than 30 pipelines failed!\s
                """;
      }

    } catch (Exception e) {
      System.out.println("[ERROR]");
      System.out.println(Arrays.toString(e.getStackTrace()));
      return null;
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
