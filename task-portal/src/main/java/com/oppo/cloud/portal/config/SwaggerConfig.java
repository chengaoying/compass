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

package com.oppo.cloud.portal.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * SpringDoc OpenAPI Configuration — only enabled in dev/test profiles to avoid exposing API docs in production.
 */
@Configuration
@Profile({"dev", "test", "default"})
public class SwaggerConfig {

    @Bean
    public OpenAPI compassOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Compass API Documentation")
                        .description("Compass AIOps diagnostic platform API")
                        .version("1.0"));
    }

    @Bean
    public GroupedOpenApi publicApi() {
        return GroupedOpenApi.builder()
                .group("compass")
                .packagesToScan("com.oppo.cloud.portal.controller")
                .build();
    }
}
