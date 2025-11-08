package com.github.adamzv.kafkamcp.support;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "kafka")
public record KafkaProperties(
    @NotBlank(message = "kafka.bootstrapServers must not be blank")
    String bootstrapServers,

    @Valid
    SecurityConfig security
) {

    public record SecurityConfig(
        boolean enabled,
        @Valid SaslConfig sasl,
        @Valid SslConfig ssl
    ) {
        public SecurityConfig {
            // Set defaults for null values
            if (sasl == null) {
                sasl = new SaslConfig(null, null, null, null);
            }
            if (ssl == null) {
                ssl = new SslConfig(null, null, null, null, null, null);
            }
        }
    }

    public record SaslConfig(
        String mechanism,
        String jaasConfig,
        String username,
        String password
    ) {
        /**
         * Validates SASL configuration and generates JAAS config if needed.
         * @return the effective JAAS configuration string
         */
        public String getEffectiveJaasConfig() {
            // If manual JAAS config provided, use it
            if (jaasConfig != null && !jaasConfig.isBlank()) {
                return jaasConfig;
            }

            // Generate JAAS config from username/password
            if (username != null && !username.isBlank() && password != null && !password.isBlank()) {
                return generateJaasConfig(mechanism, username, password);
            }

            return null;
        }

        private String generateJaasConfig(String mechanism, String username, String password) {
            if (mechanism == null) {
                return null;
            }

            return switch (mechanism.toUpperCase()) {
                case "PLAIN" -> String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    username, password
                );
                case "SCRAM-SHA-256", "SCRAM-SHA-512" -> String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                    username, password
                );
                default -> null;
            };
        }
    }

    public record SslConfig(
        String protocol,
        String truststoreLocation,
        String truststorePassword,
        String keystoreLocation,
        String keystorePassword,
        String keyPassword
    ) {
        public boolean isTruststoreConfigured() {
            return truststoreLocation != null && !truststoreLocation.isBlank();
        }

        public boolean isKeystoreConfigured() {
            return keystoreLocation != null && !keystoreLocation.isBlank();
        }
    }
}
