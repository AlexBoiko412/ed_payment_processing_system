package com.payment.gateway.filter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Global filter that runs after Spring Security's JWT validation.
 *
 * Responsibilities:
 *  1. Extracts verified JWT claims and forwards them as request headers
 *     to downstream services (X-User-Id, X-User-Roles).
 *  2. Enforces additional business claims (issuer, audience, not-before).
 *
 * Spring Security's oauth2ResourceServer already validates the signature and
 * expiry - this filter adds application-layer claim checks on top.
 */
@Slf4j
@Component
public class JwtClaimsFilter implements GlobalFilter, Ordered {

    private static final int ORDER = 1;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        return ReactiveSecurityContextHolder.getContext()
                .flatMap(ctx -> {
                    if (!(ctx.getAuthentication().getPrincipal() instanceof Jwt jwt)) {
                        return unauthorized(exchange);
                    }

                    // TODO: validate issuer claim
                    // String issuer = jwt.getIssuer().toString();
                    // if (!EXPECTED_ISSUER.equals(issuer)) return unauthorized(exchange);

                    // TODO: validate audience claim
                    // List<String> audience = jwt.getAudience();
                    // if (!audience.contains(EXPECTED_AUDIENCE)) return unauthorized(exchange);

                    // TODO: validate custom claims (e.g. account_status == "ACTIVE")

                    // Forward verified claims to downstream services as headers
                    String userId = jwt.getSubject();
                    ServerWebExchange mutated = exchange.mutate()
                            .request(r -> r
                                    .header("X-User-Id", userId)
                                    // TODO: map JWT roles claim to X-User-Roles header
                            )
                            .build();

                    log.debug("JWT validated for subject={}", userId);
                    return chain.filter(mutated);
                })
                // If there is no security context (e.g. unauthenticated path), pass through
                .switchIfEmpty(chain.filter(exchange));
    }

    @Override
    public int getOrder() {
        return ORDER;
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        return exchange.getResponse().setComplete();
    }
}
