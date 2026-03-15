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

package com.oppo.cloud.common.util;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.oppo.cloud.common.constant.Constant;
import com.oppo.cloud.common.constant.YarnAppState;
import com.oppo.cloud.common.service.RedisService;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class LogPathUtil {

    public static final String SPARK_EVENT_LOG_RUNNING_EXTENSION = ".inprogress";

    public static final String DEFAULT_LOG_DIR_SUFFIX = "logs";

    public static final String BUCKET_SUFFIX = "bucket";

    /**
     * get the redis cache from jobhistory server conf:
     * 1. yarn.nodemanager.remote-app-log-dir
     */
    public static String getYarnLogPath(String logType, String rmIp, RedisService redisService) throws Exception {
        if (!redisService.hasKey(Constant.RM_JHS_MAP)) {
            throw new Exception(String.format("search redis error,msg: can not find key %s", Constant.RM_JHS_MAP));
        }
        Map<String, String> rmJhsMap = JSON.parseObject((String) redisService.get(Constant.RM_JHS_MAP),
                new TypeReference<Map<String, String>>() {
                });
        String jhsIp = rmJhsMap.get(rmIp);
        String key = logType + jhsIp;
        if (!redisService.hasKey(key)) {
            throw new Exception(String.format("search redis error,msg: can not find key %s, rmJhsMap:%s, rmIp:%s",
                    key, rmJhsMap, rmIp));
        }
        return (String) redisService.get(key);
    }

    public static String getSparkEventLogPath(String prefixDir, String appId, String attemptId, String state, String codec) {
        String eventLogPath = String.format("%s/%s", prefixDir, appId);
        if (!StringUtils.isEmpty(attemptId)) {
            eventLogPath = String.format("%s_%s", eventLogPath, attemptId);
        }
        if (StringUtils.isNotBlank(codec)) {
            eventLogPath = String.format("%s.%s", eventLogPath, codec);
        }
        if (YarnAppState.RUNNING.toString().equals(state)) {
            eventLogPath = String.format("%s%s", eventLogPath, SPARK_EVENT_LOG_RUNNING_EXTENSION);
        }
        return eventLogPath;
    }

    public static int jobSerialNumber(String appId) {
        return Integer.parseInt(appId.substring(appId.lastIndexOf('_') + 1));
    }

    public static String getBucketDir(String appId) {
        int bucket = jobSerialNumber(appId) % 10000;
        return String.format("%04d", bucket);
    }

}
