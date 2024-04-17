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
import com.tencent.tdbank.busapi.BusClientConfig;
import com.tencent.tdbank.busapi.network.BussdkException;
import com.tencent.tdbank.busapi.network.HttpBusSender;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NgcpMetricsReporter implements MetricsReporter {
    private static final Logger log = LoggerFactory.getLogger(NgcpMetricsReporter.class);
    private String localhost;
    private boolean isLocalVisit = true;
    private String tdManagerIp;
    private int tdManagerPort;
    private String bid;
    private String tid;
    private String netTag = "all";
    // timeout in seconds
    private long timeout = 10;

    private volatile boolean shutdown = false;

    private HttpBusSender sender;

    public NgcpMetricsReporter(String localhost, boolean isLocalVisit, String tdManagerIp, int tdManagerPort,
                               String bid, String tid, String netTag, long timeout) {
        this.localhost = localhost;
        this.isLocalVisit = isLocalVisit;
        this.tdManagerIp = tdManagerIp;
        this.tdManagerPort = tdManagerPort;
        this.bid = bid;
        this.tid = tid;
        this.netTag = netTag;
        this.timeout = timeout;
        log.info("[NgcpMetricsReporter] localhost={}, isLocalVisit={}, tdManagerIp={}, tdManagerPort={}, bid={}, "
                + "netTag={}", localhost, isLocalVisit, tdManagerIp, tdManagerPort, bid, netTag);
        init();
    }

    private void init() {
        try {
            BusClientConfig  busClientConfig =
                    new BusClientConfig(localhost, isLocalVisit, tdManagerIp, tdManagerPort, bid, netTag);
            busClientConfig.setCleanHttpCacheWhenClosing(true);
            sender = new HttpBusSender(busClientConfig);
        } catch (BussdkException exception) {
            log.error("[NgcpMetricsReporter] Failed to create BusClientConfig");
        } catch (Exception e) {
            log.error("[NgcpMetricsReporter] Failed to create HttpBusSender");
        }
    }

    @Override
    public void report(String metric) {
        if (sender == null) {
            init();
        }
        if (sender == null) {
            log.error("[NgcpMetricsReporter] Failed to create HttpBusSender again.");
            return;
        }
        sender.asyncSendMessage(metric, bid, tid, System.currentTimeMillis(), timeout, TimeUnit.SECONDS,
                new NgcpSendCallback(this, metric));
    }

    @Override
    public void report(Map<String, MetricEntity> metrics) {
        //ignore
    }

    @Override
    public void stop() {
        this.shutdown = true;
    }
}
