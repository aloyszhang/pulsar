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
package org.apache.pulsar.broker.intercept;

import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataUtils;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

@Test(groups = "broker")
public class MangedLedgerInterceptorImplTest  extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(MangedLedgerInterceptorImplTest.class);
    private static final String START_INDEX = "start.index";

    @Test
    public void testAddBrokerEntryMetadata() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        int numberOfEntries = 10;
        final String ledgerAndCursorName = "topicEntryMetadataSequenceId";

        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors());

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        for ( int i = 0 ; i < numberOfEntries; i ++) {
            ledger.addEntry(("message" + i).getBytes(), MOCK_BATCH_SIZE);
        }

        assertEquals(19, ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex());
        List<Entry> entryList = cursor.readEntries(numberOfEntries);
        for (int i = 0 ; i < numberOfEntries; i ++) {
            BrokerEntryMetadata metadata =
                    Commands.parseBrokerEntryMetadataIfExist(entryList.get(i).getDataBuffer());
            assertNotNull(metadata);
            assertEquals(metadata.getIndex(), (i + 1) * MOCK_BATCH_SIZE - 1);
        }

        cursor.close();
        ledger.close();
        factory.shutdown();
    }

    @Test(timeOut = 20000)
    public void testRecoveryIndex() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors());

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setManagedLedgerInterceptor(interceptor);
        ManagedLedger ledger = factory.open("my_recovery_index_test_ledger", config);

        ledger.addEntry("dummy-entry-1".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE);

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-2".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE);

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), MOCK_BATCH_SIZE * 2 - 1);

        ledger.close();

        log.info("Closing ledger and reopening");

        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_recovery_index_test_ledger", config);

        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), MOCK_BATCH_SIZE * 2 - 1);


        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        cursor.close();
        ledger.close();
    }

    @Test
    public void testFindPositionByIndex() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        final int maxEntriesPerLedger = 5;
        int maxSequenceIdPerLedger = MOCK_BATCH_SIZE * maxEntriesPerLedger;
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors());


        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setManagedLedgerInterceptor(interceptor);
        managedLedgerConfig.setMaxEntriesPerLedger(5);

        ManagedLedger ledger = factory.open("my_ml_broker_entry_metadata_test_ledger", managedLedgerConfig);
        ManagedCursor cursor = ledger.openCursor("c1");

        long firstLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            firstLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE).getLedgerId();
        }

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 9);


        PositionImpl position = null;
        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / MOCK_BATCH_SIZE);
        }

        // roll over ledger
        long secondLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            secondLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE).getLedgerId();
        }
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 19);
        assertNotEquals(firstLedgerId, secondLedgerId);

        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / MOCK_BATCH_SIZE);
        }

        // reopen ledger
        ledger.close();
        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_ml_broker_entry_metadata_test_ledger", managedLedgerConfig);

        long thirdLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            thirdLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE).getLedgerId();
        }
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 29);
        assertNotEquals(secondLedgerId, thirdLedgerId);

        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / MOCK_BATCH_SIZE);
        }
        cursor.close();
        ledger.close();
    }

    public void testMLStartIndexLedgerRollover() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        int numberOfEntries = 10;
        final String ledgerAndCursorName = "startIndex";

        ManagedLedgerInterceptorImpl interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors());

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        assertEquals(interceptor.getStartIndex(), 0);

        for ( int i = 0 ; i < numberOfEntries; i ++) {
            ledger.addEntry(("message" + i).getBytes(), MOCK_BATCH_SIZE);
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 5);
        assertEquals(interceptor.getStartIndex(), 0);

        // managed ledger has 10 entries and 5 ledgers, each ledger has 2 entries
        // so ,the start index and entries of each ledger should like
        // LedgerId      Entries        startIndex
        // L1             0,1                0
        // L2             0,1                4
        // L3             0,1                8
        // L4             0,1                12
        // L5             0,1                16
        // for the managed ledger, the start index should be 0
        List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgerInfoList = ledger.getLedgersInfoAsList();
        for (int i = 0; i < ledgerInfoList.size(); i++) {
            MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = ledgerInfoList.get(i);
            Map<String, String> ledgerProperties = getLedgerProperties(ledgerInfo);
            long startIndex = Long.parseLong(ledgerProperties.get(START_INDEX));
            assertEquals(startIndex, i * 4);
        }

        cursor.close();
        ledger.close();
        factory.shutdown();
    }


    public void testMLStartIndexLedgerTrim() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        int numberOfEntries = 10;
        final String ledgerAndCursorName = "startIndex";

        ManagedLedgerInterceptorImpl interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors());

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        // at first startIndex should be -1
        assertEquals(interceptor.getStartIndex(), 0);

        for ( int i = 0 ; i < numberOfEntries; i ++) {
            ledger.addEntry(("message" + i).getBytes(), MOCK_BATCH_SIZE);
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 5);
        assertEquals(interceptor.getStartIndex(), 0);

        // managed ledger has 10 entries and 5 ledgers, each ledger has 2 entries
        // so ,the start index and entries of each ledger should like
        // LedgerId      Entries        startIndex
        // L1             0,1                0
        // L2             0,1                4
        // L3             0,1                8
        // L4             0,1                12
        // L5             0,1                16
        // for the managed ledger, the start index should be 0
        List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgerInfoList = ledger.getLedgersInfoAsList();
        for (int i = 0; i < ledgerInfoList.size(); i++) {
            MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = ledgerInfoList.get(i);
            Map<String, String> ledgerProperties = getLedgerProperties(ledgerInfo);
            long startIndex = Long.parseLong(ledgerProperties.get(START_INDEX));
            assertEquals(startIndex, i * 4);
        }

        // read 3 entries to make sure read position on the second ledger (L2, 0)
        // LedgerId      Entries        startIndex
        // L2             0,1                4
        // L3             0,1                8
        // L4             0,1                12
        // L5             0,1                16
        List<Entry> entryList = cursor.readEntries(3);
        cursor.markDelete(new PositionImpl(entryList.get(2).getLedgerId(), entryList.get(2).getEntryId()));

        // wait until the background trim ledger finish
        Awaitility.await().untilAsserted(() ->
                assertEquals(ledger.getLedgersInfoAsList().size(), 4));
        assertEquals(interceptor.getStartIndex(), 4);

        // (L3, 0)
        // LedgerId      Entries        startIndex
        // L3             0,1                8
        // L4             0,1                12
        // L5             0,1                16
        entryList = cursor.readEntries(2);
        cursor.markDelete(new PositionImpl(entryList.get(1).getLedgerId(), entryList.get(1).getEntryId()));

        // wait until the background trim ledger finish
        Awaitility.await().untilAsserted(() ->
                assertEquals(ledger.getLedgersInfoAsList().size(), 3));
        assertEquals(interceptor.getStartIndex(), 8);

        // (L4, 0)
        // LedgerId      Entries        startIndex
        // L4             0,1                12
        // L5             0,1                16
        entryList = cursor.readEntries(2);
        cursor.markDelete(new PositionImpl(entryList.get(1).getLedgerId(), entryList.get(1).getEntryId()));

        Awaitility.await().untilAsserted(() ->
                assertEquals(ledger.getLedgersInfoAsList().size(), 2));
        assertEquals(interceptor.getStartIndex(), 12);

        // (L5, 0)
        // LedgerId      Entries        startIndex
        // L5             0,1                16
        entryList = cursor.readEntries(2);
        cursor.markDelete(new PositionImpl(entryList.get(1).getLedgerId(), entryList.get(1).getEntryId()));

        Awaitility.await().untilAsserted(() ->
                assertEquals(ledger.getLedgersInfoAsList().size(), 1));
        assertEquals(interceptor.getStartIndex(), 16);

        // only (L5, 1) left
        // LedgerId      Entries        startIndex
        // L5             0,1                16
        entryList = cursor.readEntries(1);
        cursor.markDelete(new PositionImpl(entryList.get(0).getLedgerId(), entryList.get(0).getEntryId()));

        // after read all entries, start index of managed ledger should be 16
        assertEquals(interceptor.getStartIndex(), 16);

        cursor.close();
        ledger.close();
        factory.shutdown();
    }

    public void testMLStartIndexReopen() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        int numberOfEntries = 10;
        final String ledgerAndCursorName = "startIndex";

        ManagedLedgerInterceptorImpl interceptor =
                new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors());

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        // at first startIndex should be -1
        assertEquals(interceptor.getStartIndex(), 0);

        for ( int i = 0 ; i < numberOfEntries; i ++) {
            ledger.addEntry(("message" + i).getBytes(), MOCK_BATCH_SIZE);
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 5);
        assertEquals(interceptor.getStartIndex(), 0);

        // managed ledger has 10 entries and 5 ledgers, each ledger has 2 entries
        // so ,the start index and entries of each ledger should like
        // LedgerId      Entries        startIndex
        // L1             0,1                0
        // L2             0,1                4
        // L3             0,1                8
        // L4             0,1                12
        // L5             0,1                16
        // for the managed ledger, the start index should be 0

        // read 3 entries to make sure read position on the second ledger (L2, 0)
        // LedgerId      Entries        startIndex
        // L2             0,1                4
        // L3             0,1                8
        // L4             0,1                12
        // L5             0,1                16
        List<Entry> entryList = cursor.readEntries(3);
        cursor.markDelete(new PositionImpl(entryList.get(2).getLedgerId(), entryList.get(2).getEntryId()));

        // wait until the background trim ledger finish
        Awaitility.await().untilAsserted(() ->
                assertEquals(ledger.getLedgersInfoAsList().size(), 4));
        assertEquals(interceptor.getStartIndex(), 4);

        ledger.close();
        log.info("Closing ledger and reopening");


        // Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerImpl newLedger = (ManagedLedgerImpl) factory2.open(ledgerAndCursorName, config);
        // ManagedLedger will create a new ledger without any entries
        // now, the start index and entries of each ledger should like
        // LedgerId      Entries        startIndex
        // L2             0,1                4
        // L3             0,1                8
        // L4             0,1                12
        // L5             0,1                16
        // L6          no entries            20
        // for the managed ledger, the start index should be 0
        int ledgerInfoSize = newLedger.getLedgersInfoAsList().size();
        assertEquals(interceptor.getStartIndex(), 4);
        assertEquals(ledgerInfoSize, 5);

        MLDataFormats.ManagedLedgerInfo.LedgerInfo secondLastLedgerInfo = newLedger.getLedgersInfoAsList().get(ledgerInfoSize - 2);
        Map<String, String> ledgerProperties = getLedgerProperties(secondLastLedgerInfo);
        long startIndex = Long.parseLong(ledgerProperties.get(START_INDEX));
        assertEquals(startIndex, 16);

        MLDataFormats.ManagedLedgerInfo.LedgerInfo lastLedgerInfo = newLedger.getLedgersInfoAsList().get(ledgerInfoSize - 1);
        ledgerProperties = getLedgerProperties(lastLedgerInfo);
        startIndex = Long.parseLong(ledgerProperties.get(START_INDEX));
        assertEquals(startIndex, 20);



        ManagedCursorImpl newCursor = (ManagedCursorImpl) newLedger.openCursor(ledgerAndCursorName);

        //(L3, 0)
        // LedgerId      Entries        startIndex
        // L3             0,1                8
        // L4             0,1                12
        // L5             0,1                16
        // L6          no entries            20
        entryList = newCursor.readEntries(2);
        newCursor.markDelete(new PositionImpl(entryList.get(1).getLedgerId(), entryList.get(1).getEntryId()));

        Awaitility.await().untilAsserted(() ->
                assertEquals(newLedger.getLedgersInfoAsList().size(), 4));
        assertEquals(interceptor.getStartIndex(), 8);

        //(L4, 0)
        // LedgerId      Entries        startIndex
        // L4             0,1                12
        // L5             0,1                16
        // L6          no entries            20
        entryList = newCursor.readEntries(2);
        newCursor.markDelete(new PositionImpl(entryList.get(1).getLedgerId(), entryList.get(1).getEntryId()));

        Awaitility.await().untilAsserted(() ->
                assertEquals(newLedger.getLedgersInfoAsList().size(), 3));
        assertEquals(interceptor.getStartIndex(), 12);

        //(L5, 0)
        // LedgerId      Entries        startIndex
        // L5             0,1                16
        // L6          no entries            20
        entryList = newCursor.readEntries(2);
        newCursor.markDelete(new PositionImpl(entryList.get(1).getLedgerId(), entryList.get(1).getEntryId()));

        Awaitility.await().untilAsserted(() ->
                assertEquals(newLedger.getLedgersInfoAsList().size(), 2));
        assertEquals(interceptor.getStartIndex(), 16);

        //(L6, 0)
        // LedgerId      Entries        startIndex
        // L5             0,1                16
        // L6          no entries            20
        entryList = newCursor.readEntries(1);
        newCursor.markDelete(new PositionImpl(entryList.get(0).getLedgerId(), entryList.get(0).getEntryId()));

        Awaitility.await().untilAsserted(() ->
                assertEquals(newLedger.getLedgersInfoAsList().size(), 2));
        assertEquals(interceptor.getStartIndex(), 16);

        newCursor.close();
        newLedger.close();
        factory.shutdown();
    }

    private Map<String, String> getLedgerProperties(MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo) {
        Map<String, String> ledgerProperties = new HashMap<>();
        for (int j = 0; j < ledgerInfo.getPropertiesCount(); j++) {
            MLDataFormats.KeyValue property = ledgerInfo.getProperties(j);
            ledgerProperties.put(property.getKey(), property.getValue());
        }
        return ledgerProperties;
    }

    public static Set<BrokerEntryMetadataInterceptor> getBrokerEntryMetadataInterceptors() {
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        return BrokerEntryMetadataUtils.loadBrokerEntryMetadataInterceptors(interceptorNames,
                Thread.currentThread().getContextClassLoader());
    }

    static class IndexSearchPredicate implements com.google.common.base.Predicate<Entry> {

        long indexToSearch = -1;
        public IndexSearchPredicate(long indexToSearch) {
            this.indexToSearch = indexToSearch;
        }

        @Override
        public boolean apply(@Nullable Entry entry) {
            try {
                BrokerEntryMetadata brokerEntryMetadata = Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer());
                return brokerEntryMetadata.getIndex() < indexToSearch;
            } catch (Exception e) {
                log.error("Error deserialize message for message position find", e);
            } finally {
                entry.release();
            }
            return false;
        }
    }

}
