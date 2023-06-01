package io.vantiq.svcconnector;

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
