package io.vantiq.svcconnector;

import java.util.Map;

class SvcConnSvrMessage {
  public static final String WS_PING = "ping";
  public static final String WS_PONG = "pong";

  public String requestId;
  public String procName;
  public Map<String, Object> params;
}
