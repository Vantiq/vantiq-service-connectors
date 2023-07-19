package io.vantiq.svcconnector;

import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for starting the service connector server
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */
@Builder
public class SvcConnectorConfig {
    @Getter
    private String storageManagerClassName;
}
