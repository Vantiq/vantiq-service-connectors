package io.vantiq.svcconnector;

/**
 * The message format for responses sent from the storage manager service connector
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
