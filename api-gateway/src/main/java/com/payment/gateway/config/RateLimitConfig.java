package com.payment.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * Redis-backed rate limiting configuration for Spring Cloud Gateway.
 *
 * The {@code RequestRateLimiter} filter references {@code ipKeyResolver} by name
 * in application.yml. Replace with a JWT-subject resolver in production so that
 * rate limits apply per authenticated user, not per IP
 */
@Configuration
public class RateLimitConfig {

    /**
     * Rate-limit key based on the remote IP address.
     * TODO: switch to JWT subject key resolver for per-user limits:
     *   exchange -> ReactiveSecurityContextHolder.getContext()
     *       .map(ctx -> ctx.getAuthentication().getName())
     */
    @Bean
    public KeyResolver ipKeyResolver() {
        return exchange -> Mono.justOrEmpty(
                exchange.getRequest().getRemoteAddress())
                .map(addr -> addr.getAddress().getHostAddress())
                .defaultIfEmpty("unknown");
    }

    // TODO: add a jwtSubjectKeyResolver() bean for per-user rate limiting
    // TODO: configure separate rate limit tiers (standard vs premium accounts)
}
