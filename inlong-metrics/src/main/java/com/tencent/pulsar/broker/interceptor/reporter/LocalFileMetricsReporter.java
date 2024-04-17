/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.tencent.pulsar.broker.interceptor.reporter;

import com.tencent.pulsar.broker.interceptor.MetricEntity;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reporter for reporting metric to local file.
 * */
public class LocalFileMetricsReporter implements MetricsReporter {

    private final static Logger log = LoggerFactory.getLogger(LocalFileMetricsReporter.class);

    @Override
    public void report(String metric) {
        log.info(metric);
    }

    @Override
    public void report(Map<String, MetricEntity> metrics) {
        // ignore
    }

    @Override
    public void stop() {
        // ignore
    }
}
