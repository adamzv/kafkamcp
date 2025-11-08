package com.github.adamzv.kafkamcp.support;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecurityConfigHelperTest {

    @Test
    void applySecurity_disabledSecurity_doesNotApplyConfig() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            false,
            new KafkaProperties.SaslConfig("PLAIN", null, "user", "pass"),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props).doesNotContainKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    }

    @Test
    void applySecurity_nullConfig_doesNothing() {
        Properties props = new Properties();

        SecurityConfigHelper.applySecurityConfig(props, null);

        assertThat(props).isEmpty();
    }

    @Test
    void applySecurity_enabledWithNoSaslOrSsl_throwsException() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig(null, null, null, null),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        );

        assertThatThrownBy(() -> SecurityConfigHelper.applySecurityConfig(props, securityConfig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("no SASL or SSL configuration provided");
    }

    @Test
    void applySasl_plainWithUsernamePassword_generatesJaasConfig() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig("PLAIN", null, "alice", "secret"),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props)
            .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
            .containsEntry(SaslConfigs.SASL_MECHANISM, "PLAIN")
            .containsKey(SaslConfigs.SASL_JAAS_CONFIG);

        String jaasConfig = props.getProperty(SaslConfigs.SASL_JAAS_CONFIG);
        assertThat(jaasConfig)
            .contains("PlainLoginModule")
            .contains("username=\"alice\"")
            .contains("password=\"secret\"");
    }

    @Test
    void applySasl_scramSha256WithUsernamePassword_generatesJaasConfig() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig("SCRAM-SHA-256", null, "bob", "password123"),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props)
            .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
            .containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
            .containsKey(SaslConfigs.SASL_JAAS_CONFIG);

        String jaasConfig = props.getProperty(SaslConfigs.SASL_JAAS_CONFIG);
        assertThat(jaasConfig)
            .contains("ScramLoginModule")
            .contains("username=\"bob\"")
            .contains("password=\"password123\"");
    }

    @Test
    void applySasl_scramSha512WithUsernamePassword_generatesJaasConfig() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig("SCRAM-SHA-512", null, "charlie", "secure!"),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props)
            .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
            .containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512")
            .containsKey(SaslConfigs.SASL_JAAS_CONFIG);

        String jaasConfig = props.getProperty(SaslConfigs.SASL_JAAS_CONFIG);
        assertThat(jaasConfig)
            .contains("ScramLoginModule")
            .contains("username=\"charlie\"")
            .contains("password=\"secure!\"");
    }

    @Test
    void applySasl_manualJaasConfig_usesProvidedConfig() {
        Properties props = new Properties();
        String customJaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"manual\" password=\"config\";";
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig("PLAIN", customJaas, null, null),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props)
            .containsEntry(SaslConfigs.SASL_JAAS_CONFIG, customJaas);
    }

    @Test
    void applySasl_manualJaasConfigOverridesUsernamePassword() {
        Properties props = new Properties();
        String customJaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"manual\" password=\"config\";";
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig("PLAIN", customJaas, "alice", "secret"),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props)
            .containsEntry(SaslConfigs.SASL_JAAS_CONFIG, customJaas);
    }

    @Test
    void applySasl_noCredentials_throwsException() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig("PLAIN", null, null, null),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        );

        assertThatThrownBy(() -> SecurityConfigHelper.applySecurityConfig(props, securityConfig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("no credentials provided");
    }

    @Test
    void applySasl_unsupportedMechanism_throwsException() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig("GSSAPI", null, "user", "pass"),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        );

        assertThatThrownBy(() -> SecurityConfigHelper.applySecurityConfig(props, securityConfig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported SASL mechanism: GSSAPI");
    }

    @Test
    void applySsl_truststoreOnly_configuresSsl() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig(null, null, null, null),
            new KafkaProperties.SslConfig("TLSv1.3", "/etc/kafka/truststore.jks", "trustpass", null, null, null)
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props)
            .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
            .containsEntry(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.3")
            .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/kafka/truststore.jks")
            .containsEntry(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "trustpass")
            .doesNotContainKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    }

    @Test
    void applySsl_keystoreWithTruststore_configuresMutualTls() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig(null, null, null, null),
            new KafkaProperties.SslConfig(
                "TLSv1.3",
                "/etc/kafka/truststore.jks", "trustpass",
                "/etc/kafka/keystore.jks", "keypass", "keyentry"
            )
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props)
            .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
            .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/kafka/truststore.jks")
            .containsEntry(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "trustpass")
            .containsEntry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/kafka/keystore.jks")
            .containsEntry(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "keypass")
            .containsEntry(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "keyentry");
    }

    @Test
    void applySsl_keystoreWithoutPassword_throwsException() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig(null, null, null, null),
            new KafkaProperties.SslConfig(
                "TLSv1.3",
                "/etc/kafka/truststore.jks", "trustpass",
                "/etc/kafka/keystore.jks", null, null
            )
        );

        assertThatThrownBy(() -> SecurityConfigHelper.applySecurityConfig(props, securityConfig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("keystorePassword is missing");
    }

    @Test
    void applySaslAndSsl_combinedConfig_usesSaslSslProtocol() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig("PLAIN", null, "user", "pass"),
            new KafkaProperties.SslConfig("TLSv1.3", "/etc/kafka/truststore.jks", "trustpass", null, null, null)
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props)
            .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
            .containsEntry(SaslConfigs.SASL_MECHANISM, "PLAIN")
            .containsKey(SaslConfigs.SASL_JAAS_CONFIG)
            .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/kafka/truststore.jks");
    }

    @Test
    void applySaslAndSsl_withMutualTls_configuresAll() {
        Properties props = new Properties();
        var securityConfig = new KafkaProperties.SecurityConfig(
            true,
            new KafkaProperties.SaslConfig("SCRAM-SHA-256", null, "user", "pass"),
            new KafkaProperties.SslConfig(
                "TLSv1.3",
                "/etc/kafka/truststore.jks", "trustpass",
                "/etc/kafka/keystore.jks", "keypass", "keyentry"
            )
        );

        SecurityConfigHelper.applySecurityConfig(props, securityConfig);

        assertThat(props)
            .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
            .containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
            .containsKey(SaslConfigs.SASL_JAAS_CONFIG)
            .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/kafka/truststore.jks")
            .containsEntry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/kafka/keystore.jks");
    }
}
