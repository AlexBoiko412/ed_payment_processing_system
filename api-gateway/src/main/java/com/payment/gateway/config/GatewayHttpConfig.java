package com.payment.gateway.config;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.netty.http.client.HttpClient;

import javax.net.ssl.SSLException;

/**
 * Dev-only: configure the gateway's outbound HTTP client to skip TLS verification
 * for internal services (which use self-signed certificates generated at build time).
 *
 * The public-facing TLS (browser -> gateway) is unaffected - that certificate
 * is verified normally by the caller's browser or curl.
 *
 * In production: remove this bean and add the internal CA cert to the JVM trust
 * store (JAVA_OPTS=-Djavax.net.ssl.trustStore=...) instead.
 */
@Configuration
public class GatewayHttpConfig {

    @Bean
    public HttpClient httpClient() throws SSLException {
        SslContext sslContext = SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
        return HttpClient.create()
                .secure(spec -> spec.sslContext(sslContext));
    }
}
