package com.mediaiq.mixpanel_slack_alert_service.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BlockUtil {
  private List<Map<String, Object>> blocks;

  public BlockUtil() {
    reset();
  }

  public void reset() {
    this.blocks = new ArrayList<>();
  }

  public void addBlock(Map<String, Object> block) {
    if (block != null) {
      blocks.add(block);
    }
  }

  public void addBlocks(List<Map<String, Object>> newBlocks) {
    if (newBlocks != null && !newBlocks.isEmpty()) {
      blocks.addAll(newBlocks);
    }
  }

  public Map<String, Object> toSlackPayload() {
    return Map.of("blocks", blocks);
  }

  public void addTerminator() {
    addBlock(Map.of(
            "type", "divider"
    ));

    addBlock(Map.of(
            "type", "context",
            "elements", List.of(Map.of(
                    "type", "mrkdwn",
                    "text", "*End of Alerts* - Next batch will be sent in an hour"
            ))
    ));
  }
}
