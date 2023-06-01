package io.vantiq.testServiceConnector;

import io.vantiq.svcconnector.SvcConnectorConfig;
import io.vantiq.svcconnector.SvcConnectorServer;

public class TestSvcConnectorMain {
    
    public static void main(String[] args) {
        new TestSvcConnectorMain().run();
    }
    
    public void run() {
        SvcConnectorConfig config = SvcConnectorConfig.builder()
                //.storageManagerClassName(MemoryStorageManagerImpl.class.getCanonicalName())
                .storageManagerClassName(TestStorageManagerImpl.class.getCanonicalName())
                .build();

        new SvcConnectorServer().start(config);
    }
}
