package io.vantiq.svcconnector;


import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.ext.web.Session;
import io.vertx.ext.web.sstore.SessionStore;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Mix-in for a service that wants to manage the lifecycle of a session.
 * <p>
 * Created by sfitts on 10/24/2015.
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */
public class SessionCreator {
  public static final long SESSION_TIMEOUT = TimeUnit.MINUTES.toMillis(15);

  private final Vertx vertx;
  private final SessionStore sessionStore;

  public SessionCreator(Vertx vertx, SessionStore sessionStore) {
    this.vertx = vertx;
    this.sessionStore = sessionStore;
  }

  public void startSession(final Session session, final Map sessionInfo, final Handler<AsyncResult<Void>> handler) {
    // Install timer which will keep the session active
    final long keepAlive = vertx.setPeriodic(session.timeout() / 3, getKeepaliveHandler(session.id()));
    session.put("keepAlive", keepAlive);

    // Put session in the store
    sessionStore.put(session, result -> {
      // Figure out result, execute side effects, let the handler know
      if (!result.succeeded()) {
        // Cancel the timer we installed
        vertx.cancelTimer(keepAlive);
      }

      if (handler != null) {
        handler.handle(result);
      }
    });
  }

  public Handler<Long> getKeepaliveHandler(final String sessionId) {
    return timerId -> {
      try {
        sessionStore.get(sessionId, getResult -> {
          // Make sure the get succeeded
          if (getResult.failed()) {
            vertx.cancelTimer(timerId);
            return;
          }

          // Get the session and make sure it is not null
          Session session = getResult.result();
          if (session == null) {
            vertx.cancelTimer(timerId);
            return;
          }

          // Make the session as accessed and update
          session.setAccessed();
          sessionStore.put(session, result -> {
            // See if we failed or not
            if (result.failed()) {
              // Version mismatch, retry
              if (result.cause() instanceof NoStackTraceThrowable &&
                result.cause().getMessage().contains("Version")) {
                getKeepaliveHandler(sessionId).handle(timerId);
                return;
              }
            } else {
            }
          });
        });

      } catch (Exception ignored) {
        // Ignore and move on (we'll retry again)
      }
    };
  }

  public void closeSession(final String sessionId) {
    // Find the session prior to removing it
    sessionStore.get(sessionId, result -> {
      // If we can't find the session, abort
      if (result.failed() || result.result() == null) {
        return;
      }

      // Disable keepalive timer so session can be collected
      Session session = result.result();
      Long keepAlive = session.get("keepAlive");
      if (keepAlive != null && keepAlive != 0) {
        vertx.cancelTimer(keepAlive);
      }
    });
  }
}
