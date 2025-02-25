package com.mediaiq.mixpanel_slack_alert_service.Util;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateUtil {

  public static String getTodayDateInIST() {
    LocalDate todayIST = LocalDate.now(ZoneId.of("Asia/Kolkata"));
    return todayIST.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
  }

}
