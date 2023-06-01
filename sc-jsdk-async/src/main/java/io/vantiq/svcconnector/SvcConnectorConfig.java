package io.vantiq.svcconnector;

import lombok.Builder;
import lombok.Getter;

@Builder
/**
 * Configuration for starting the service connector server
 */
public class SvcConnectorConfig {
    @Getter
    private String storageManagerClassName;
}
