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

package com.oppo.cloud.application.util;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * string processing tool
 */
@Slf4j
public class StringUtil {

    /**
     * Convert a template with {@code ${key}} placeholders into a parameterized SQL query.
     *
     * <p>Returns a two-element array:
     * <ol>
     *   <li>index 0: the SQL string with {@code ${key}} replaced by {@code ?}</li>
     *   <li>index 1: an {@code Object[]} of parameter values in placeholder order</li>
     * </ol>
     *
     * <p>Usage:
     * <pre>
     *   Object[] result = StringUtil.toParameterizedQuery(query, data);
     *   String sql = (String) result[0];
     *   Object[] args = (Object[]) result[1];
     *   jdbcTemplate.queryForMap(sql, args);
     * </pre>
     *
     * <p>This replaces the previous {@code replaceParams} string-substitution approach,
     * which was vulnerable to SQL injection when field values contained SQL metacharacters.
     */
    public static Object[] toParameterizedQuery(String template, Map<String, Object> params) {
        List<Object> args = new java.util.ArrayList<>();
        // Replace each ${key} with ? and collect the corresponding value
        java.util.regex.Pattern p = java.util.regex.Pattern.compile("\\$\\{(\\w+)}");
        java.util.regex.Matcher m = p.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            Object value = params.get(key);
            if (value == null || value instanceof List) {
                log.error("Missing or invalid parameter for SQL template key '{}', params: {}", key, params);
                m.appendReplacement(sb, "?");
                args.add(null);
            } else {
                m.appendReplacement(sb, "?");
                args.add(value);
            }
        }
        m.appendTail(sb);
        return new Object[]{sb.toString(), args.toArray()};
    }

    /**
     * @deprecated Use {@link #toParameterizedQuery(String, Map)} to avoid SQL injection.
     */
    @Deprecated
    public static String replaceParams(String template, Map<String, Object> params) {
        for (String key : params.keySet()) {
            if (params.get(key) == null) {
                continue;
            }
            if (params.get(key) instanceof List) {
                log.error("Wrong DataType for replaceParams, data: {} ", params.get(key));
                continue;
            }
            template = template.replace("${" + key + "}", params.get(key).toString());
        }
        return template;
    }
}
