package io.vantiq.svcconnector;

import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for starting the service connector server
 */
@Builder
public class SvcConnectorConfig {
    @Getter
    private String storageManagerClassName;
}
