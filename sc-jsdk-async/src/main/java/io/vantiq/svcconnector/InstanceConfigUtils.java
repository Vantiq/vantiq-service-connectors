/*
 * Copyright (c) 2022 Vantiq, Inc.
 *
 * All rights reserved.
 */

package io.vantiq.svcconnector;

import com.google.common.io.Resources;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class InstanceConfigUtils {
    public static final String CONFIG_WAS_PROGRAMMATIC = "configWasProgrammatic";
    public static final String SERVER_CONFIG_FILENAME = "config.properties";
    public static final String SERVER_SECRETS_FILENAME = "secrets.properties";
    public static final String PORT_PROP_NAME = "port";
    public static final String TCP_PROBE_PORT_NAME = "tcpProbePort";
    public static final String SECRETS_DIR_PROP = "io.vantiq.secretsDir";
    public static final String CONFIG_DIR_PROP = "io.vantiq.configDir";
    public static final Integer DEFAULT_PORT = 8888;

    // The properties object containing the data from the server configuration file
    private Properties serverConfigProperties;

    public Properties loadServerSecrets() {
        return loadServerSecrets(SERVER_SECRETS_FILENAME);
    }
    
    public Properties loadServerConfig() {
        return loadServerConfig(SERVER_CONFIG_FILENAME);
    }

    public Properties loadServerSecrets(String fileName) {
        Properties secrets = new Properties();
        val resourceUrl = Resources.getResource(fileName);
        ByteArrayInputStream propertyStream = null;
        try {
            propertyStream = new ByteArrayInputStream(Resources.toByteArray(resourceUrl));
            secrets.load(propertyStream);
        } catch (IOException ignored) {
            // Error loading options, continue
        }
        if (propertyStream == null) {
            File secretsFile = new File(getSecretsDir(), fileName);
            try {
                secrets.load(new FileInputStream(secretsFile));
            } catch (IOException ignored) {
                // give up...
            }
        }
        return secrets;
    }

    /**
     * Turn the given configuration file into a {@link Properties} object.
     *
     * @param fileName The name of the configuration file holding the server configuration.
     * @return The properties specified in the file.
     */
    public Properties loadServerConfig(String fileName) {
        synchronized (this) {
            boolean wasProgrammatic = false;
            if (serverConfigProperties != null) {
                if (serverConfigProperties.get(CONFIG_WAS_PROGRAMMATIC) instanceof Boolean) {
                    wasProgrammatic = (Boolean) serverConfigProperties.get(CONFIG_WAS_PROGRAMMATIC);
                }
            }

            // If config props were provided programmatically, don't override them.  Simply return
            // what's already there.
            if (wasProgrammatic) {
                return serverConfigProperties;
            }

            // Otherwise, re-read the file
            InputStream propertyStream = null;
            try {
                val resourceUrl = Resources.getResource(fileName);
                propertyStream = new ByteArrayInputStream(Resources.toByteArray(resourceUrl));
            } catch (IllegalArgumentException | IOException iae) {
                // Error loading options, continue
            }
            serverConfigProperties = new Properties();

            try {
                if (propertyStream != null) {
                    serverConfigProperties.load(propertyStream);
                }
                String configDir = getConfigDir();
                log.trace("Configuration dir {}", configDir);
                if (configDir != null) {
                    File configFile = new File(configDir, fileName);
                    if (configFile.exists()) {
                        log.trace("Loading configuration file {}", configFile.getAbsolutePath());
                        try {
                            // subsequent load will override if keys exist
                            serverConfigProperties.load(new FileInputStream(configFile));
                        } catch (IOException e) {
                            // Error loading options, warn and continue
                            log.warn("Failed to load configuration from file {}", configFile, e);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Could not find valid server configuration file. Expected location directory: "
                        + getConfigDir() + " file name: " + fileName, e);
            } catch (Exception e) {
                throw new RuntimeException("Error occurred when trying to read the server configuration file. "
                        + "Please ensure it is formatted properly.", e);
            }
            return serverConfigProperties;
        }
    }

    /**
     * Helper method used to get the Port whether or not it is specified in the config.properties
     *
     * @return An Integer for the port value provided in the config.properties or the default if not
     */
    public Integer obtainPrimaryPort() {
        int port = DEFAULT_PORT;
        if (serverConfigProperties.getProperty(PORT_PROP_NAME) != null) {
            port = Integer.parseInt(serverConfigProperties.getProperty(PORT_PROP_NAME));
        }
        return port;
    }

    public String obtainDefaultDatabase() {
        Properties localServerConfigProps;
        // Get a local copy of the props while synchronized in case they are reloaded
        synchronized (this) {
            localServerConfigProps = serverConfigProperties;
        }
        return localServerConfigProps.getProperty("defaultDatabase");
    }
    
    /**
     * Helper method used to get the TCP Probe Port if specified in the server.config
     *
     * @return An Integer for the port value provided in the server.config file, or null if non was specified.
     */
    public Integer obtainTCPProbePort() {
        Properties localServerConfigProps;

        // Get a local copy of the props while synchronized
        synchronized (this) {
            localServerConfigProps = serverConfigProperties;
        }

        if (localServerConfigProps != null) {
            // Grab the property and return result
            String portString = localServerConfigProps.getProperty(TCP_PROBE_PORT_NAME);
            if (portString != null) {
                return Integer.parseInt(portString);
            }
        } else {
            throw new RuntimeException("Error occurred when checking for the tcpProbePort property. The " +
                    "server.config properties have not yet been captured. Before checking for specific properties, " +
                    "the 'obtainServerConfig' method must first be called.");
        }

        return null;
    }

    /**
     * Method used to clear the local copy of server.config properties
     */
    public void clearServerConfigProperties() {
        synchronized (this) {
            serverConfigProperties = null;
        }
    }
   
    public static String getSecretsDir() {
        return System.getProperty(SECRETS_DIR_PROP, "./secrets");
    }

    public static String getConfigDir() {
        return System.getProperty(CONFIG_DIR_PROP, "./config");
    }
}
