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
package com.tencent.pulsar.broker.interceptor;

import static com.tencent.pulsar.broker.interceptor.InLongMetricsManager.maxMetricCacheSize;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InLongMetricsInterceptor implements BrokerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(InLongMetricsInterceptor.class);
    public static final long INVALID_METRIC_TIME = -1L;

    // <PositionImpl, EntryMetricsProperties>
    // PositionImpl : ledgerId (8), entryId(8), ackSet(8) * ackSetSize
    // EntryMetricProperties : timestamp (8), batchSize (4), entryMsgSize (4)
    // ackSet size not included, it will calculate by ackSetSize * 8 for every individual PositionImpl
    public static final int CONSUMER_METRIC_PROPERTIES_BASE_SIZE = 32;


    // map to cache all the metrics needed for consume metrics if  recordMetricsWhenSendMessageToConsumer is false
    // if recordMetricsWhenSendMessageToConsumer is true, this map will not be used
    private final ConcurrentHashMap<Consumer, ConcurrentHashMap<PositionImpl, EntryMetricProperties>>
            msgInfoSendToConsumer = new ConcurrentHashMap<>();
    private final AtomicLong consumerMetricsSize = new AtomicLong(0);

    InLongMetricsManager inLongMetricsManager;

    // if record consume metrics at the point of sending message to consumer instead of ack message
    // this means, for consumer, we can't need to keep the metricsInfo(msgInfoSendToConsumer) in memory
    boolean recordMetricsWhenSendMessageToConsumer = false;


    @Override
    public void beforeSendMessage(Subscription subscription, Entry entry, long[] ackSet, MessageMetadata msgMetadata,
                                  Consumer consumer) {
        if (!inLongMetricsManager.metricsReportEnabled())  {
            return;
        }

        if (isSystemTopic(subscription.getTopicName())) {
            return;
        }
        // if recordMetricsWhenSendMessageToConsumer is true, record metrics directly
        if (recordMetricsWhenSendMessageToConsumer) {
            InLongMetricsManager.record(InLongMetricsTypes.Consumer.getType(),
                    consumer.getStats().getAddress(),
                    consumer.getSubscription().getName(),
                    consumer.getSubscription().getTopicName(),
                    msgMetadata.hasEventTime() ? msgMetadata.getEventTime() : msgMetadata.getPublishTime(),
                    msgMetadata.getNumMessagesInBatch(),
                    1,
                    entry.getLength());
        } else {
            if (consumerMetricsSize.get() > maxMetricCacheSize) {
                log.warn("[{}] [{}] Cached consumerMetricsSize {} exceed maxMetricCacheSize {}, discard new metric" +
                                "msgInfoSendToConsumer size {}",
                        consumer.getSubscription().getName(),
                        consumer.getStats().getConsumerName(),
                        consumerMetricsSize,
                        maxMetricCacheSize,
                        msgInfoSendToConsumer.size());
                return;
            }
            ConcurrentHashMap<PositionImpl, EntryMetricProperties> entryProperties =
                    msgInfoSendToConsumer.computeIfAbsent(consumer, k -> new ConcurrentHashMap<>());
            entryProperties.put(new PositionImpl(entry.getLedgerId(), entry.getEntryId(), ackSet),
                    new EntryMetricProperties(msgMetadata.hasEventTime()
                            ? msgMetadata.getEventTime() : msgMetadata.getPublishTime(),
                            msgMetadata.getNumMessagesInBatch(),
                            entry.getLength()));
            consumerMetricsSize.addAndGet(CONSUMER_METRIC_PROPERTIES_BASE_SIZE
                    + (ackSet == null ? 0L : ackSet.length * 8L));
        }
    }

    @Override
    public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
        if (!inLongMetricsManager.metricsReportEnabled())  {
            return;
        }
        if (!recordMetricsWhenSendMessageToConsumer) {
            msgInfoSendToConsumer.computeIfAbsent(consumer, k -> new ConcurrentHashMap<>());
        }
    }

    @Override
    public void consumerClosed(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
        if (!inLongMetricsManager.metricsReportEnabled())  {
            return;
        }
        if (!recordMetricsWhenSendMessageToConsumer) {
            ConcurrentHashMap<PositionImpl, EntryMetricProperties> consumerMetrics =
                    msgInfoSendToConsumer.remove(consumer);
            if (consumerMetrics != null) {
                long size = 0;
                for (PositionImpl position : consumerMetrics.keySet()) {
                    size += CONSUMER_METRIC_PROPERTIES_BASE_SIZE
                            + (position.getAckSet() == null ? 0L : position.getAckSet().length * 8L);
                }
                consumerMetricsSize.addAndGet(-size);
            }
        }
    }

    @Override
    public void onMessagePublish(Producer producer, ByteBuf headersAndPayload,
                                        Topic.PublishContext publishContext) {
        if (!inLongMetricsManager.metricsReportEnabled())  {
            return;
        }
        if (isSystemTopic(producer.getTopic().getName())) {
            return;
        }

        // for TD-Bank minute metrics, transaction messages are excluded
        MessageMetadata msgMetadata = Commands.peekMessageMetadata(headersAndPayload, "metric-sub", -1);
        if (msgMetadata != null) {
            publishContext.setEntryTimestamp(
                    msgMetadata.hasEventTime() ? msgMetadata.getEventTime() : msgMetadata.getPublishTime());
        } else {
            log.warn("[{}] Producer {} failed to get message metric time.",
                    producer.getTopic().getName(), producer.getProducerName());
        }
    }

    @Override
    public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId, long entryId,
                                Topic.PublishContext publishContext) {
        if (!inLongMetricsManager.metricsReportEnabled())  {
            return;
        }

        if (isSystemTopic(producer.getTopic().getName())) {
            return;
        }

        if (publishContext.getEntryTimestamp() != INVALID_METRIC_TIME) {
            InLongMetricsManager.record(InLongMetricsTypes.Producer.getType(), producer.getStats().getAddress(), "",
                    producer.getTopic().getName(), publishContext.getEntryTimestamp(),
                    publishContext.getNumberOfMessages(), 1, publishContext.getMsgSize());
        } else {
            log.warn("[{}] Invalid metric timestamp {}.", producer.getTopic().getName(), producer.getProducerName());
        }
    }

    @Override
    public void messagePersistent(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId, long entryId,
            PublishContext publishContext) {
        if (!inLongMetricsManager.metricsReportEnabled())  {
            return;
        }

        if (isSystemTopic(producer.getTopic().getName())) {
            return;
        }

        if (publishContext.getEntryTimestamp() != INVALID_METRIC_TIME) {
            InLongMetricsManager.record(InLongMetricsTypes.Persistent.getType(), producer.getStats().getAddress(),
                    "", producer.getTopic().getName(), publishContext.getEntryTimestamp(),
                    publishContext.getNumberOfMessages(), 1, publishContext.getMsgSize());
        } else {
            log.warn("[{}] Invalid metric timestamp {}.", producer.getTopic().getName(), producer.getProducerName());
        }
    }

    @Override
    public void messageAcked(ServerCnx cnx, Consumer consumer, CommandAck ackCmd) {
        if (!inLongMetricsManager.metricsReportEnabled())  {
            return;
        }

        if (isSystemTopic(consumer.getSubscription().getTopicName())) {
            return;
        }

        if (recordMetricsWhenSendMessageToConsumer) {
            return;
        }
        ConcurrentHashMap<PositionImpl, EntryMetricProperties> pendingAcks = msgInfoSendToConsumer.get(consumer);
        if (pendingAcks == null) {
            log.warn("[{}-{}] No pending acks for consumer{} found. ",
                    consumer.getSubscription().getTopicName(),
                    consumer.getSubscription().getName(),
                    consumer.consumerName());
            return;
        }

        CommandAck.AckType ackType = ackCmd.getAckType();

        if (ackType == CommandAck.AckType.Cumulative) {
            PositionImpl position = PositionImpl.EARLIEST;
            if (ackCmd.getMessageIdsCount() == 1) {
                position = getPositionFromMessageId(ackCmd.getMessageIdAt(0));
            }
            if (!pendingAcks.containsKey(position)) {
                log.warn("[{}-{}] consumer {} no pending ack found for {}:{}",
                        consumer.getSubscription().getTopicName(),
                        consumer.getSubscription().getName(),
                        consumer.consumerName(),
                        position.getLedgerId(), position.getEntryId());
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] removing pending cumulative ack at position {}",
                        consumer.getSubscription().getTopicName(), consumer.getSubscription().getName(), position);
            }

            List<PositionImpl> ackedPositions = new ArrayList<>();
            final PositionImpl maxAckPosition = position;
            LongAdder totalAckSetSize = new LongAdder();
            // process positions before ack-ed position
            pendingAcks.forEach((p, entryMetricProperties) -> {
                if (p.getLedgerId() < maxAckPosition.getLedgerId()
                        || (p.getLedgerId() == maxAckPosition.getLedgerId()
                        && p.getEntryId() < maxAckPosition.getEntryId())) {
                    ackedPositions.add(p);
                    totalAckSetSize.add(p.getAckSet() == null ? 0L : p.getAckSet().length * 8L);
                    if (entryMetricProperties != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}-{}] recording consume {} metric from clientIp {}, timestamp {}, "
                                            + "batchSize {}, entryMsgSize {}",
                                    consumer.getSubscription().getTopicName(),
                                    consumer.getSubscription().getName(),
                                    consumer.consumerName(),
                                    consumer.getStats().getAddress(),
                                    entryMetricProperties.getTimesatmp(),
                                    entryMetricProperties.getBatchSize(),
                                    entryMetricProperties.getEntryMsgSize());
                        }
                        InLongMetricsManager.record(InLongMetricsTypes.Consumer.getType(),
                                consumer.getStats().getAddress(),
                                consumer.getSubscription().getName(),
                                consumer.getSubscription().getTopicName(),
                                entryMetricProperties.getTimesatmp(),
                                entryMetricProperties.getBatchSize(),
                                1,
                                entryMetricProperties.getEntryMsgSize());
                    } else {
                        log.warn("[{}-{}] consumer {} no entry timestamp and msgSize found for {}:{}",
                                consumer.getSubscription().getTopicName(),
                                consumer.getSubscription().getName(),
                                consumer.consumerName(),
                                p.getLedgerId(), p.getEntryId());
                    }
                }
            });
            // remove ack-ed positions
            ackedPositions.forEach(pendingAcks::remove);
            consumerMetricsSize.addAndGet(
                    -((long) ackedPositions.size() * CONSUMER_METRIC_PROPERTIES_BASE_SIZE
                            + totalAckSetSize.longValue()));

            // process ack-ed position
            EntryMetricProperties ackedEntryMetricProperties = pendingAcks.get(position);
            long ackedMsgs = position.getAckSet() != null
                    ? position.getAckSet().length : Math.max(1, ackedEntryMetricProperties.getBatchSize());
            if (maxAckPosition.getAckSet() != null) {
                // get the ack-ed message count
                long pendingAckMsgs = ackedEntryMetricProperties.getBatchSize() - ackedMsgs;
                // if every message in this entry has been ack-ed
                if (pendingAckMsgs == 0) {
                    pendingAcks.remove(position);
                    consumerMetricsSize.addAndGet(-(CONSUMER_METRIC_PROPERTIES_BASE_SIZE
                            + (position.getAckSet() == null ? 0L : position.getAckSet().length * 8L)));
                    InLongMetricsManager.record(InLongMetricsTypes.Consumer.getType(),
                            consumer.getStats().getAddress(),
                            consumer.getSubscription().getName(),
                            consumer.getSubscription().getTopicName(),
                            ackedEntryMetricProperties.getTimesatmp(),
                            ackedEntryMetricProperties.getBatchSize(),
                            1,
                            ackedEntryMetricProperties.getEntryMsgSize());
                }
            } else { // process non-batch messages
                pendingAcks.remove(position);
                consumerMetricsSize.addAndGet(-(CONSUMER_METRIC_PROPERTIES_BASE_SIZE
                        + (position.getAckSet() == null ? 0L : position.getAckSet().length * 8L)));
                InLongMetricsManager.record(InLongMetricsTypes.Consumer.getType(),
                        consumer.getStats().getAddress(),
                        consumer.getSubscription().getName(),
                        consumer.getSubscription().getTopicName(),
                        ackedEntryMetricProperties.getTimesatmp(),
                        ackedEntryMetricProperties.getBatchSize(),
                        1,
                        ackedEntryMetricProperties.getEntryMsgSize());
            }
        } else { // for individual ack
            for (int i = 0; i < ackCmd.getMessageIdsCount(); i++) {
                MessageIdData msgId = ackCmd.getMessageIdAt(i);
                if (msgId.getAckSetsCount() == 0) {
                    PositionImpl position = getPositionFromMessageId(msgId);
                    EntryMetricProperties metricProperties = pendingAcks.get(getPositionFromMessageId(msgId));
                    if (metricProperties == null){
                        log.warn("[{}-{}] consumer {} no entry timestamp and msgSize found for {}:{}",
                                consumer.getSubscription().getTopicName(),
                                consumer.getSubscription().getName(),
                                consumer.consumerName(),
                                msgId.getLedgerId(), msgId.getEntryId());
                        continue;
                    }
                    pendingAcks.remove(position);
                    consumerMetricsSize.addAndGet(-(CONSUMER_METRIC_PROPERTIES_BASE_SIZE
                            + (position.getAckSet() == null ? 0L : position.getAckSet().length * 8L)));
                    InLongMetricsManager.record(InLongMetricsTypes.Consumer.getType(),
                            consumer.getStats().getAddress(),
                            consumer.getSubscription().getName(),
                            consumer.getSubscription().getTopicName(),
                            metricProperties.getTimesatmp(),
                            metricProperties.getBatchSize(),
                            1,
                            metricProperties.getEntryMsgSize());
                }
            }
        }


    }

    private PositionImpl getPositionFromMessageId(MessageIdData msgId) {
        if (msgId.getAckSetsCount() > 0) {
            long[] ackSets = new long[msgId.getAckSetsCount()];
            for (int j = 0; j < msgId.getAckSetsCount(); j++) {
                ackSets[j] = msgId.getAckSetAt(j);
            }
           return new PositionImpl(msgId.getLedgerId(), msgId.getEntryId(), ackSets);
        } else {
            return new PositionImpl(msgId.getLedgerId(), msgId.getEntryId());
        }
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {

    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {

    }

    @Override
    public void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException {

    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response)
            throws IOException, ServletException {

    }

    @Override
    public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        BrokerInterceptor.super.onFilter(request, response, chain);
    }

    @Override
    public void onInLongMetricsReportTargetsModify(List<String> metricsDestinations) {
        inLongMetricsManager.onInLongMetricsReportTargetsModify(metricsDestinations);
        if (!inLongMetricsManager.metricsReportEnabled()) {
            log.info("[InlongMetricInterceptor] inlong metrics report is disabled, clean all metrics");
            msgInfoSendToConsumer.clear();
        }
    }

    @Override
    public void onInLongMetricsMaxMetricsCacheSizeModify(long maxMetricsCacheSize) {
        inLongMetricsManager.onInLongMetricsMaxMetricsCacheSizeModify(maxMetricsCacheSize);
    }

    @Override
    public void onInLongMetricsCacheFlushTimeoutModify(long cacheFlushTimeout) {
        inLongMetricsManager.onInLongMetricsCacheFlushTimeoutModify(cacheFlushTimeout);
    }

    @Override
    public void onInLongConsumeMetricsRecordTypeModify(boolean recordMetricsWhenSendToConsume) {
        this.recordMetricsWhenSendMessageToConsumer = recordMetricsWhenSendToConsume;
        log.info("[InlongMetricInterceptor] recordMetricsWhenSendMessageToConsumer: {}",
                recordMetricsWhenSendMessageToConsumer);
        if (recordMetricsWhenSendToConsume) {
            log.info("[InlongMetricInterceptor] recordMetricsWhenSendMessageToConsumer is enabled, "
                    + "clean cached consumer metric information");
            msgInfoSendToConsumer.clear();
            consumerMetricsSize.set(0);
        }
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {
        inLongMetricsManager = new InLongMetricsManager(pulsarService.getConfiguration());
        inLongMetricsManager.start();
        this.recordMetricsWhenSendMessageToConsumer =
                pulsarService.getConfiguration().isRecordMetricsWhenSendMessageToConsumer();
        log.info("[InlongMetricInterceptor] Initialize recordMetricsWhenSendMessageToConsumer: {}",
                recordMetricsWhenSendMessageToConsumer);
    }

    @Override
    public void close() {

    }

    private boolean isSystemTopic(String fullTopicName) {
        if (fullTopicName == null || fullTopicName.isEmpty()) {
            return false;
        }
        String localTopicName = fullTopicName.split("/")[fullTopicName.split("/").length - 1];
        int endIndex = localTopicName.contains("-partition-")
                ? localTopicName.lastIndexOf("-partition-")
                : localTopicName.length();
        localTopicName = localTopicName.substring(0, endIndex);
        return localTopicName.equalsIgnoreCase("__transaction_buffer_snapshot")
                || localTopicName.equalsIgnoreCase("healthcheck");
    }
}
