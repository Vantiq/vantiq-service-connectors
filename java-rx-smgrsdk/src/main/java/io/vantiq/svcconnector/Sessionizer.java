package io.vantiq.svcconnector;

import io.vertx.ext.web.sstore.SessionStore;

/**
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */
public interface Sessionizer {
  SessionStore getSessionStore();
  SessionCreator getSessionCreator();
}
