package io.vantiq.svcconnector;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/**
 * The message format for messages sent to the storage manager service connector
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SvcConnSvrMessage {
  public static final String WS_PING = "ping";
  public static final String WS_PONG = "pong";

  public String requestId;
  public String procName;
  public Map<String, Object> params;
  public Boolean isSystemRequest;
}
