package io.vantiq.svcconnector;

/**
 * The message format for responses sent from the storage manager service connector
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */
class SvcConnSvrResponse {
  public String requestId;
  public Object result;
  public String errorMsg;
  public boolean isEOF = true;

  private SvcConnSvrResponse() {

  }

  public SvcConnSvrResponse(String requestId) {
    this.requestId = requestId;
  }
}
