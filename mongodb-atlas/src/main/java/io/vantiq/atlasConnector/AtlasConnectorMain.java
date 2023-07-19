package io.vantiq.atlasConnector;

import io.vantiq.svcconnector.SvcConnectorConfig;
import io.vantiq.svcconnector.SvcConnectorServer;

public class AtlasConnectorMain {

    public static void main(String[] args) {
        new AtlasConnectorMain().run();
    }

    public void run() {
        SvcConnectorConfig config = SvcConnectorConfig.builder()
                .storageManagerClassName(AtlasStorageMgr.class.getCanonicalName())
                .build();

        new SvcConnectorServer().start(config);
    }
}
