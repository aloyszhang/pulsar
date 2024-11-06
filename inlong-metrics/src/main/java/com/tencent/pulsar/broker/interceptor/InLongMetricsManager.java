/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.02
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.tencent.pulsar.broker.interceptor;

import com.tencent.pulsar.broker.interceptor.reporter.LocalFileMetricsReporter;
import com.tencent.pulsar.broker.interceptor.reporter.NgcpMetricsReporter;
import com.tencent.pulsar.broker.interceptor.reporter.ReportDestination;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Report metric for producers and consumer, which will do report at pre-defined time granularity.
 *
 * <p/> metric has a key and a value which ie (see {@link MetricEntity})
 *
 * <p/> metric has two destinations, one is local metric file and another is Ngcp.
 * */
public class InLongMetricsManager {

    private static final Logger log = LoggerFactory.getLogger(InLongMetricsManager.class);

    public static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    private static final String localIp = getLocalIp();
    private static final long DEFAULT_SEND_TIMEOUT = 10;
    private static final long DEFAULT_INITIAL_DELAY = 10;
    private static final long DEFAULT_REPORT_INTERVAL = 60;
    public static final long DEFAULT_MAX_METRICS_CACHE_SIZE = 512 * 1024 * 1024;
    // timeout for one-round flush task
    public static final long DEFAULT_CACHE_FLUSH_TIMEOUT_MILLIS = 50 * 1000;
    public static final String DELIMITER_LOCAL_FILE = "#";
    public static final String DELIMITER_NGCP = "|";
    public static final String INVALID_KEY = "invalid_metric_key";

    private static final AtomicLong localFileCacheSizeCounter = new AtomicLong(0);

    private static final AtomicLong ngcpCacheSizeCounter = new AtomicLong(0);

    public volatile static long maxMetricCacheSize;
    public volatile static long flushTimeout;

    // <metricKey, metricEntity>
    private static final AtomicReference<ConcurrentHashMap<String, MetricEntity>> localFileMetricsRef =
            new AtomicReference<>();
    private static final AtomicReference<ConcurrentHashMap<String, MetricEntity>> ngcpMetricsRef =
            new AtomicReference<>();
    private static final ReentrantReadWriteLock localFileLock = new ReentrantReadWriteLock(true);
    private static final ReentrantReadWriteLock ngcpLock = new ReentrantReadWriteLock(true);
    private static final ScheduledExecutorService metricService =  Executors.newScheduledThreadPool(2,
            new DefaultThreadFactory("metrics-reporter"));

    private static String clusterName;
    public volatile static boolean localFileReportEnabled = false;
    public volatile static boolean ngcpReportEnabled = false;

    private LocalFileMetricsReporter localFileMetricReporter;
    private NgcpMetricsReporter ngcpMetricsReporter;
    private long initialDelay;
    private long reportInterval;
    private long sendTimeout;
    private String tdManagerIp;
    private int tdManagerPort;
    private String bid;
    private String tid;

    public InLongMetricsManager(ServiceConfiguration configuration) {

        localFileMetricsRef.set(new ConcurrentHashMap<>());
        ngcpMetricsRef.set(new ConcurrentHashMap<>());
        clusterName = configuration.getClusterName();

        List<String> destinations = configuration.getMetricsDestinations();
        if (destinations == null || destinations.size() == 0) {
            log.warn("[InLongMetricsManager] No metrics destination found, please check InLongMetricsManager "
                    + "metricDestinations in broker.conf");
            return;
        }

        localFileReportEnabled = destinations.contains(ReportDestination.LOCAL_FILE.getDestination());

        ngcpReportEnabled = destinations.contains(ReportDestination.NGCP.getDestination());

        if (!localFileReportEnabled && !ngcpReportEnabled) {
            log.warn("[InLongMetricsManager] Neither local file nor ngcp reporter enabled.");
        }

        maxMetricCacheSize = configuration.getMaxMetricsCacheSize() <= 0
                ? DEFAULT_MAX_METRICS_CACHE_SIZE : configuration.getMaxMetricsCacheSize();

        flushTimeout = configuration.getMetricsCacheFlushTimeout() <= 0
                ? DEFAULT_CACHE_FLUSH_TIMEOUT_MILLIS : configuration.getMetricsCacheFlushTimeout();

        this.initialDelay = configuration.getMetricsReportInitialDelay() <= 0
                ? DEFAULT_INITIAL_DELAY : configuration.getMetricsReportInitialDelay();

        this.reportInterval = configuration.getMetricsReportInterval() <= 0
                ? DEFAULT_REPORT_INTERVAL : configuration.getMetricsReportInterval();

        this.sendTimeout = configuration.getNgcpSendTimeout() <= 0
                ? DEFAULT_SEND_TIMEOUT : configuration.getNgcpSendTimeout();

        this.tdManagerIp = configuration.getTdManagerIp();
        this.tdManagerPort = configuration.getTdManagerPort();
        this.bid = configuration.getNgcpTopicBid();
        this.tid = configuration.getNgcpTopicTid();

        if (ngcpReportEnabled) {
            if (tdManagerIp == null || tdManagerIp.isEmpty()) {
                log.error("[InLongMetricsManager] tdManagerIp is null or empty, please check InLongMetricsManager "
                        + "tdManagerIp in broker.conf");
                return;
            }
            if (tdManagerPort <= 0) {
                log.error("[InLongMetricsManager] tdManagerPort is invalid, please check InLongMetricsManager "
                        + "tdManagerPort in broker.conf");
                return;
            }
            if (bid == null || bid.isEmpty()) {
                log.error("[InLongMetricsManager] bid is null or empty, please check InLongMetricsManager "
                        + "bid in broker.conf");
                return;
            }
            if (tid == null || tid.isEmpty()) {
                log.error("[InLongMetricsManager] tid is null or empty, please check InLongMetricsManager "
                        + "tid in broker.conf");
                return;
            }
        }

        log.info("[InLongMetricsManager] \n"
                        + " destinations={}, localFileReportEnabled={}, ngcpReportEnabled={},\n"
                        + " initialDelay={}, reportInterval={}, sendTimeout={}, \n"
                        + " tdManagerIp={}, tdManagerPort={}, bid={}, tid={}\n"
                        + " maxMetricCacheSize={}, flushTimeout={}",
                destinations, localFileReportEnabled, ngcpReportEnabled, initialDelay, reportInterval, sendTimeout,
                tdManagerIp, tdManagerPort, tid, bid, maxMetricCacheSize, flushTimeout);
    }

    public void start() {

        if (localFileReportEnabled) {
            localFileMetricReporter = new LocalFileMetricsReporter();
        }
        metricService.scheduleAtFixedRate(this::reportMetricToLocalFile,
                initialDelay, reportInterval, TimeUnit.SECONDS);

        if (ngcpReportEnabled) {
            ngcpMetricsReporter = new NgcpMetricsReporter(localIp,
                    true, tdManagerIp, tdManagerPort, bid, tid, "all", sendTimeout);
        }
        metricService.scheduleAtFixedRate(this::reportMetricToNgcp,
                initialDelay, reportInterval, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new IndexShutDownHook(this));
    }


    public void onInLongMetricsReportTargetsModify(List<String> metricsDestinations) {
        boolean originalLocalFileReportEnabled = localFileReportEnabled;
        boolean originalNgcpReportEnabled = ngcpReportEnabled;
        localFileReportEnabled = metricsDestinations.contains(ReportDestination.LOCAL_FILE.getDestination());
        ngcpReportEnabled = metricsDestinations.contains(ReportDestination.NGCP.getDestination());
        log.info("[InLongMetricsManager] onInLongMetricsReportTargetsModify, localFileReportEnabled={}, "
                        + "ngcpReportEnabled={}", localFileReportEnabled, ngcpReportEnabled);
        // clear cache if no reporter enabled
        if (originalLocalFileReportEnabled && !localFileReportEnabled) {
            if (localFileMetricsRef.get() != null) {
                localFileMetricsRef.get().clear();
                localFileMetricsRef.set(null);
            }
        }

        if (originalNgcpReportEnabled && !ngcpReportEnabled) {
            if (ngcpMetricsRef.get() != null) {
                ngcpMetricsRef.get().clear();
                ngcpMetricsRef.set(null);
            }
        }

        // if no reporter enabled before, initialize cache and reporter if needed
        if (!originalLocalFileReportEnabled && localFileReportEnabled) {
            localFileMetricsRef.set(new ConcurrentHashMap<>());
            if (localFileMetricReporter == null) {
                log.info("[InLongMetricsManager] localFileMetricReporter is null, initialize it");
                localFileMetricReporter = new LocalFileMetricsReporter();
            }
        }
        if (!originalNgcpReportEnabled && ngcpReportEnabled) {
            ngcpMetricsRef.set(new ConcurrentHashMap<>());
            if (ngcpMetricsReporter == null) {
                log.info("[InLongMetricsManager] ngcpMetricsReporter is null, initialize it");
                ngcpMetricsReporter = new NgcpMetricsReporter(localIp,
                        true, tdManagerIp, tdManagerPort, bid, tid, "all", sendTimeout);
            }
        }
    }

    public void onInLongMetricsMaxMetricsCacheSizeModify(long maxMetricsCacheSize) {
        maxMetricCacheSize = maxMetricsCacheSize <= 0 ? DEFAULT_MAX_METRICS_CACHE_SIZE : maxMetricsCacheSize;
        log.info("[InLongMetricsManager] onInLongMetricsMaxMetricsCacheSizeModify, maxMetricCacheSize={}",
                maxMetricCacheSize);
    }

    public void onInLongMetricsCacheFlushTimeoutModify(long cacheFlushTimeout) {
        flushTimeout = cacheFlushTimeout <= 0 ? DEFAULT_CACHE_FLUSH_TIMEOUT_MILLIS : cacheFlushTimeout;
        log.info("[InLongMetricsManager] onInLongMetricsCacheFlushTimeoutModify, flushTimeout={}", flushTimeout);
    }

    /**
     * Cache metrics in memory map.
     * */
    public static void record(final String type, final String ip, final String group, final String topic,
                              long dataTimeMs, long msgNum, final long entryNum, final long msgSize) {
        if (localFileReportEnabled) {
            if (localFileCacheSizeCounter.get() < maxMetricCacheSize) {
                cacheLocalFileMetric(type, ip, group, topic, dataTimeMs, msgNum, entryNum, msgSize);
            } else {
                log.warn("[InLongMetricsManager] Local_file metrics cache size {} exceeds maxCacheSize {}, discard it.",
                        localFileCacheSizeCounter.get(), maxMetricCacheSize);
            }
        }

        // if NGCP is enabled, only record produce and consume metrics(not including persistent metrics)
        if (ngcpReportEnabled && isProduceOrConsumeMetric(type)) {
            if (ngcpCacheSizeCounter.get() < maxMetricCacheSize) {
                cacheNgcpMetric(type, ip, group, topic, dataTimeMs, msgNum, entryNum, msgSize);
            } else {
                log.warn("[InLongMetricsManager] Ngcp metrics cache size {} exceeds maxCacheSize {}, discard it.",
                        ngcpCacheSizeCounter.get(), maxMetricCacheSize);
            }
        }
    }

    private static boolean isProduceOrConsumeMetric(String type) {
        if (type == null || type.isEmpty()) {
            return false;
        }
        return type.equals(InLongMetricsTypes.Producer.getType())
                || type.equals(InLongMetricsTypes.Consumer.getType());
    }


    public static void cacheLocalFileMetric(final String type, final String clientIp,
                                            final String group, final String topic,
                                            long dataTimeMs, long msgNum,
                                            final long entryNum, final long msgSize) {
        String timeStamp =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(dataTimeMs),
                        ZoneId.systemDefault()).format(DATETIME_FORMAT);
        String key = getLocalFileKey(type, group, topic, clientIp, timeStamp);

        ReentrantReadWriteLock.ReadLock readLock = localFileLock.readLock();
        try {
            if (readLock.tryLock(5000, TimeUnit.MILLISECONDS)) {
                ConcurrentHashMap<String, MetricEntity> localFileMetrics =
                        InLongMetricsManager.localFileMetricsRef.get();
                MetricEntity metricEntity = localFileMetrics.computeIfAbsent(key, k -> new MetricEntity());
                if (0 == metricEntity.entryNum.get()) {
                    localFileCacheSizeCounter.addAndGet(key.length() + 24);
                }
                metricEntity.entryNum.addAndGet(entryNum);
                metricEntity.messageSize.addAndGet(msgSize);
                metricEntity.messageNum.addAndGet(msgNum);
            }
        } catch (Exception e) {
            log.error("[InLongMetricsManager] Cache local file metrics exception.", e);
        } finally {
            readLock.unlock();
        }
    }

    public static void cacheNgcpMetric(final String type, final String ip, final String group, final String topic,
                                       long dataTimeMs, long msgNum, final long entryNum, final long msgSize) {
        ReentrantReadWriteLock.ReadLock readLock = ngcpLock.readLock();
        try {
            String key = getNgcpKey(type, group, topic, ip, dataTimeMs);
            if (readLock.tryLock(5000, TimeUnit.MILLISECONDS)) {
                ConcurrentHashMap<String, MetricEntity> ngcpMetrics = InLongMetricsManager.ngcpMetricsRef.get();
                MetricEntity metricEntity = ngcpMetrics.computeIfAbsent(key, k -> new MetricEntity());
                if (0 == metricEntity.entryNum.get()) {
                    ngcpCacheSizeCounter.addAndGet(key.length() + 24);
                }
                metricEntity.entryNum.addAndGet(entryNum);
                metricEntity.messageSize.addAndGet(msgSize);
                metricEntity.messageNum.addAndGet(msgNum);
            }
        } catch (Exception e) {
            log.error("[InLongMetricsManager] Cache ngcp metrics exception.", e);
        } finally {
            readLock.unlock();
        }
    }



    public void reportMetricToLocalFile() {
        if (!localFileReportEnabled) {
            log.info("[InLongMetricsManager] Local file metric is disable, just skip report.");
            return;
        }
        long start = Instant.now().getNano();
        long localFileCacheSize = localFileCacheSizeCounter.get();
        ConcurrentHashMap<String, MetricEntity> metricsToReport = null;
        ReentrantReadWriteLock.WriteLock writeLock = ngcpLock.writeLock();
        try {
            if (writeLock.tryLock(5000, TimeUnit.MILLISECONDS)) {
                metricsToReport = localFileMetricsRef.get();
                localFileMetricsRef.set(new ConcurrentHashMap<>());
            }
        } catch (InterruptedException e) {
            log.error("[InLongMetricsManager] Interrupted exception when get metrics for local file.");
        } finally {
            writeLock.unlock();
            localFileCacheSizeCounter.set(0);
        }
        if (metricsToReport != null) {
            log.info("[InLongMetricsManager] starting report {} metrics to local file...", metricsToReport.size());
            int metricsReportedCount = 0;
            for (Map.Entry<String, MetricEntity> entry : metricsToReport.entrySet()) {
                String key = entry.getKey();
                MetricEntity metricResult = entry.getValue();
                if (key != null && metricResult != null) {
                    localFileMetricReporter.report(getLocalFileMetric(key, metricResult));
                }
                metricsReportedCount++;
                if ((Instant.now().getNano() - start) / 1000000 > flushTimeout) {
                    log.warn("[InLongMetricsManager] report metrics to local file timeout, with total {} flushed {}",
                            metricsToReport.size(), metricsReportedCount);
                    break;
                }
            }
        }
        log.info("[InLongMetricsManager] report metrics to local file cost {} ms with size {}",
                (Instant.now().getNano() - start) / 1000000, localFileCacheSize);
    }

    public void reportMetricToNgcp() {
        if (!ngcpReportEnabled) {
            log.info("[InLongMetricsManager] Ngcp metric is disable, just skip report.");
            return;
        }
        long start = Instant.now().getNano();
        long ngcpCacheSize = ngcpCacheSizeCounter.get();
        ConcurrentHashMap<String, MetricEntity> metricsToReport = null;
        ReentrantReadWriteLock.WriteLock writeLock = ngcpLock.writeLock();
        try {
            if (writeLock.tryLock(5000, TimeUnit.MILLISECONDS)) {
                metricsToReport = ngcpMetricsRef.get();
                ngcpMetricsRef.set(new ConcurrentHashMap<>());
            }
        } catch (InterruptedException e) {
            log.error("[InLongMetricsManager] Interrupted exception when get metrics for ngcp.");
        } finally {
            writeLock.unlock();
            ngcpCacheSizeCounter.set(0);
        }
        if (metricsToReport != null) {
            log.info("[InLongMetricsManager] starting report {} metrics to ngcp...", metricsToReport.size());
            int metricsReportedCount = 0;
            for (Map.Entry<String, MetricEntity> entry : metricsToReport.entrySet()) {
                String key = entry.getKey();
                MetricEntity metricResult = entry.getValue();
                if (key != null && metricResult != null) {
                    ngcpMetricsReporter.report(getNgcpMetric(key, metricResult));
                }
                metricsReportedCount++;
                if ((Instant.now().getNano() - start) / 1000000 > flushTimeout) {
                    log.warn("[InLongMetricsManager] report metrics to ngcp timeout, with total {} flushed {}",
                            metricsToReport.size(), metricsReportedCount);
                    break;
                }
            }
        }
        log.info("[InLongMetricsManager] report metrics to ngcp cost {} ms with size {}",
                (Instant.now().getNano() - start) / 1000000, ngcpCacheSize);
    }


    private static String getLocalFileKey(String type,
                                          String group,
                                          String topic,
                                          String clientIp,
                                          String timeStamp) {
        try {
            TopicName topicName = TopicName.get(topic);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder
                    .append(type).append(DELIMITER_LOCAL_FILE)
                    .append(DELIMITER_LOCAL_FILE) // ignore domain
                    .append(topicName.getTenant()).append(DELIMITER_LOCAL_FILE)
                    .append(topicName.getNamespacePortion()).append(DELIMITER_LOCAL_FILE);

            addTopicName(topicName, stringBuilder, DELIMITER_LOCAL_FILE);

            stringBuilder
                    .append(group).append(DELIMITER_LOCAL_FILE)
                    .append(clientIp.replaceAll("/", "").split(":")[0]).append(DELIMITER_LOCAL_FILE)
                    .append(localIp).append(DELIMITER_LOCAL_FILE)
                    .append(timeStamp);

            return stringBuilder.toString();
        } catch (Throwable t) {
            return INVALID_KEY;
        }
    }

    private static String getNgcpKey(String type,
            String group,
            String topic,
            String ip,
            long timeStamp) {
        try {
            // format timestamp to minute level
            long formatTimestamp = timeStamp - (timeStamp % (1000 * 60));
            TopicName topicName = TopicName.get(topic);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder
                    .append(clusterName).append(DELIMITER_NGCP) // first element is clusterName for NGCP metrics
                    .append(type).append(DELIMITER_NGCP)
                    .append(DELIMITER_NGCP) // ignore domain
                    .append(topicName.getTenant()).append(DELIMITER_NGCP)
                    .append(topicName.getNamespacePortion()).append(DELIMITER_NGCP);

            addTopicName(topicName, stringBuilder, DELIMITER_NGCP);

            stringBuilder
                    .append(group).append(DELIMITER_NGCP)
                    .append(ip.replaceAll("/", "").split(":")[0]).append(DELIMITER_NGCP)
                    .append(localIp).append(DELIMITER_NGCP)
                    .append(formatTimestamp);

            return stringBuilder.toString();
        } catch (Throwable t) {
            log.error("Build NGCP metric key failed.", t);
            return INVALID_KEY;
        }
    }

    public static void addTopicName(TopicName topicName, StringBuilder stringBuilder, String delimiter) {
        if (topicName.isPartitioned()) {
            String[] topicElements = topicName.getPartitionedTopicName().split("/");
            stringBuilder
                    .append(topicElements[topicElements.length - 1]).append(delimiter)
                    .append(topicName.getPartitionIndex());
        } else {
            stringBuilder
                    .append(topicName.getLocalName()).append(delimiter);
        }
        stringBuilder.append(delimiter);
    }

    private static String getLocalFileMetric(String key, MetricEntity metricEntity) {
        return key + DELIMITER_LOCAL_FILE
                + metricEntity.messageNum + DELIMITER_LOCAL_FILE
                + metricEntity.entryNum + DELIMITER_LOCAL_FILE
                + metricEntity.messageSize;
    }

    private static String getNgcpMetric(String key, MetricEntity metricEntity) {
        return key + DELIMITER_NGCP
                + metricEntity.messageNum + DELIMITER_NGCP
                + metricEntity.entryNum + DELIMITER_NGCP
                + metricEntity.messageSize;
    }

    public static String getLocalIp() {
        Enumeration<NetworkInterface> allInterface;
        String localIp = null;
        try {
            allInterface = NetworkInterface.getNetworkInterfaces();
            for (; allInterface.hasMoreElements(); ) {
                NetworkInterface oneInterface = allInterface.nextElement();
                if (oneInterface.isLoopback() || !oneInterface.isUp() || oneInterface.isVirtual()) {
                    continue;
                }

                Enumeration<InetAddress> allAddress = oneInterface.getInetAddresses();
                while (allAddress.hasMoreElements()) {
                    InetAddress oneAddress = allAddress.nextElement();
                    if (oneAddress instanceof Inet6Address) {
                        continue;
                    }
                    localIp = oneAddress.getHostAddress();
                    if (localIp == null || localIp.isEmpty() || localIp.equals("127.0.0.1")) {
                        continue;
                    }
                    return localIp;
                }
            }
        } catch (SocketException e) {
            log.warn("Fail to get local ip.", e);
        }
        return "localhost";
    }

    public boolean metricsReportEnabled() {
        return localFileReportEnabled || ngcpReportEnabled;
    }

    private final static class IndexShutDownHook extends Thread {
        private InLongMetricsManager InLongMetricManager;

        public IndexShutDownHook(InLongMetricsManager InLongMetricManager) {
            this.InLongMetricManager = InLongMetricManager;
        }

        @Override
        public void run() {
            InLongMetricManager.reportMetricToLocalFile();
            InLongMetricManager.reportMetricToNgcp();
        }
    }
}
