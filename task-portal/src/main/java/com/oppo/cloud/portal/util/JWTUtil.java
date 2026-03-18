/*
 * Copyright 2023 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.cloud.portal.util;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.common.util.DateUtil;
import com.oppo.cloud.model.UserInfo;
import com.oppo.cloud.portal.domain.task.UserResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.UUID;

@Component
public class JWTUtil {

    private static final String BLACKLIST_PREFIX = "jwt:blacklist:";

    @Value("${custom.jwt.expireDay}")
    private int expireDay = 7;
    @Value("${custom.jwt.secret}")
    private String secret;

    @Autowired
    private RedisService redisService;

    public String createToken(UserInfo user) throws Exception {
        boolean isAdmin = user.getIsAdmin() == 0;
        return JWT.create()
                .withIssuer("compass")
                .withJWTId(UUID.randomUUID().toString())
                .withClaim("userId", user.getId())
                .withClaim("username", user.getUsername())
                .withClaim("isAdmin", isAdmin)
                .withClaim("schedulerType", user.getSchedulerType())
                .withExpiresAt(DateUtil.getOffsetDate(new Date(), expireDay))
                .sign(Algorithm.HMAC256(secret));
    }

    public UserResponse verifyToken(String token) {
        JWTVerifier verifier = JWT.require(Algorithm.HMAC256(secret))
                .withIssuer("compass")
                .build();
        DecodedJWT decodedJWT = verifier.verify(token);

        // Check if token has been invalidated (logout)
        String jti = decodedJWT.getId();
        if (jti != null && Boolean.TRUE.equals(redisService.hasKey(BLACKLIST_PREFIX + jti))) {
            throw new RuntimeException("Token has been invalidated");
        }

        Integer userId = decodedJWT.getClaim("userId").asInt();
        String username = decodedJWT.getClaim("username").asString();
        Boolean isAdmin = decodedJWT.getClaim("isAdmin").asBoolean();
        String schedulerType = decodedJWT.getClaim("schedulerType").asString();
        UserResponse userInfo = new UserResponse();
        userInfo.setUserId(userId);
        userInfo.setUsername(username);
        userInfo.setAdmin(isAdmin);
        userInfo.setSchedulerType(schedulerType);
        return userInfo;
    }

    /**
     * Invalidate a token by adding its JTI to the Redis blacklist.
     * The blacklist entry expires when the token would have expired.
     */
    public void invalidateToken(String token) {
        try {
            DecodedJWT decodedJWT = JWT.decode(token);
            String jti = decodedJWT.getId();
            if (jti != null) {
                long ttlSeconds = (decodedJWT.getExpiresAt().getTime() - System.currentTimeMillis()) / 1000;
                if (ttlSeconds > 0) {
                    redisService.set(BLACKLIST_PREFIX + jti, "1", ttlSeconds);
                }
            }
        } catch (Exception ignored) {
            // Token may already be expired or malformed — ignore
        }
    }
}
