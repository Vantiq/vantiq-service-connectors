package io.vantiq.svcconnector;

import io.vertx.ext.web.sstore.SessionStore;

public interface Sessionizer {
  SessionStore getSessionStore();
  SessionCreator getSessionCreator();
}
