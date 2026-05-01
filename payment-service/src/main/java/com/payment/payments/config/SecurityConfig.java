package com.payment.payments.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Payment-service is an internal service - JWT validation is done at the
 * API Gateway. This service trusts the X-User-Id header injected by the
 * gateway and does not re-validate tokens.
 *
 * In production this service should only be reachable from the gateway's
 * private network (VPC / Kubernetes NetworkPolicy), not from the internet.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
                .build();
    }
}
