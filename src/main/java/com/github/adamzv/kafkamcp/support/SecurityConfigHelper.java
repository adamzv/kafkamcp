package com.github.adamzv.kafkamcp.support;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Helper class for applying Kafka security configuration to client properties.
 * Supports SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) and SSL/TLS encryption.
 */
public final class SecurityConfigHelper {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfigHelper.class);

    private SecurityConfigHelper() {
        // Utility class, prevent instantiation
    }

    /**
     * Applies security configuration to Kafka client properties.
     *
     * @param properties the Kafka client properties to configure
     * @param securityConfig the security configuration to apply
     * @throws IllegalArgumentException if security is enabled but configuration is invalid
     */
    public static void applySecurityConfig(Properties properties, KafkaProperties.SecurityConfig securityConfig) {
        if (securityConfig == null || !securityConfig.enabled()) {
            log.debug("Kafka security is disabled");
            return;
        }

        log.info("Applying Kafka security configuration");

        KafkaProperties.SaslConfig sasl = securityConfig.sasl();
        KafkaProperties.SslConfig ssl = securityConfig.ssl();

        boolean hasSasl = sasl != null && sasl.mechanism() != null && !sasl.mechanism().isBlank();
        boolean hasSsl = ssl != null && ssl.isTruststoreConfigured();

        if (!hasSasl && !hasSsl) {
            throw new IllegalArgumentException(
                "Security is enabled but no SASL or SSL configuration provided. " +
                "Configure either kafka.security.sasl.mechanism or kafka.security.ssl.truststoreLocation"
            );
        }

        // Determine security protocol
        SecurityProtocol protocol = determineSecurityProtocol(hasSasl, hasSsl);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.name);
        log.info("Using security protocol: {}", protocol.name);

        // Apply SASL configuration if present
        if (hasSasl) {
            applySaslConfig(properties, sasl);
        }

        // Apply SSL configuration if present
        if (hasSsl) {
            applySslConfig(properties, ssl);
        }
    }

    private static SecurityProtocol determineSecurityProtocol(boolean hasSasl, boolean hasSsl) {
        if (hasSasl && hasSsl) {
            return SecurityProtocol.SASL_SSL;
        } else if (hasSasl) {
            return SecurityProtocol.SASL_PLAINTEXT;
        } else {
            return SecurityProtocol.SSL;
        }
    }

    private static void applySaslConfig(Properties properties, KafkaProperties.SaslConfig sasl) {
        String mechanism = sasl.mechanism();

        // Validate mechanism first
        validateSaslMechanism(mechanism);

        String jaasConfig = sasl.getEffectiveJaasConfig();

        if (jaasConfig == null || jaasConfig.isBlank()) {
            throw new IllegalArgumentException(
                "SASL is enabled but no credentials provided. " +
                "Set kafka.security.sasl.jaasConfig or both kafka.security.sasl.username and kafka.security.sasl.password"
            );
        }

        properties.put(SaslConfigs.SASL_MECHANISM, mechanism);
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        log.info("SASL authentication configured with mechanism: {}", mechanism);
        log.debug("SASL JAAS config applied (credentials hidden for security)");
    }

    private static void validateSaslMechanism(String mechanism) {
        if (mechanism == null || mechanism.isBlank()) {
            throw new IllegalArgumentException("SASL mechanism must not be blank");
        }

        String upperMechanism = mechanism.toUpperCase();
        if (!upperMechanism.equals("PLAIN") &&
            !upperMechanism.equals("SCRAM-SHA-256") &&
            !upperMechanism.equals("SCRAM-SHA-512")) {
            throw new IllegalArgumentException(
                "Unsupported SASL mechanism: " + mechanism + ". " +
                "Supported mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512"
            );
        }
    }

    private static void applySslConfig(Properties properties, KafkaProperties.SslConfig ssl) {
        // SSL Protocol
        String protocol = ssl.protocol();
        if (protocol != null && !protocol.isBlank()) {
            properties.put(SslConfigs.SSL_PROTOCOL_CONFIG, protocol);
            log.debug("SSL protocol: {}", protocol);
        }

        // Truststore (server certificate validation)
        if (ssl.isTruststoreConfigured()) {
            properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ssl.truststoreLocation());
            if (ssl.truststorePassword() != null && !ssl.truststorePassword().isBlank()) {
                properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ssl.truststorePassword());
            }
            log.info("SSL truststore configured: {}", ssl.truststoreLocation());
        }

        // Keystore (client certificate - mutual TLS)
        if (ssl.isKeystoreConfigured()) {
            properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ssl.keystoreLocation());

            if (ssl.keystorePassword() == null || ssl.keystorePassword().isBlank()) {
                throw new IllegalArgumentException(
                    "Keystore is configured but keystorePassword is missing. " +
                    "Set kafka.security.ssl.keystorePassword"
                );
            }
            properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ssl.keystorePassword());

            if (ssl.keyPassword() != null && !ssl.keyPassword().isBlank()) {
                properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, ssl.keyPassword());
            }

            log.info("SSL keystore configured (mutual TLS): {}", ssl.keystoreLocation());
        }
    }
}
