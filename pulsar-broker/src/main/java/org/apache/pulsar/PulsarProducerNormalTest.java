package org.apache.pulsar;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;

public class PulsarProducerNormalTest {

    public static void main(String[] args) {
        try {

            System.setProperty("io.netty.hostsFileRefreshInterval", "1000");

            System.out.println(InetAddress.getLocalHost().getHostName());

            PulsarClient client = PulsarClient.builder()
                    .enableTransaction(true)
                    .serviceUrl("pulsar://test.pulsar.com:6650").connectionsPerBroker(1)
                    .ioThreads(1)
                    .operationTimeout(5, TimeUnit.MINUTES)
                    .build();

            String topic = "persistent://compaction/compaction/compaction_test-partition-0";

            Producer<byte[]> producer = client.newProducer()
                    .topic(topic)
                    .create();

            Transaction txn = client.newTransaction().build().get();

            System.out.println("start");

            for (int i = 0; i < 200000000; i++) {
                try {

                    String key = "C";
                    if (System.currentTimeMillis() % 2 == 0) {
                        key = "D";
                    }

                    String message = "My message" + i + " | " + System.currentTimeMillis();
                    long start = System.currentTimeMillis();
                    MessageId send = producer.newMessage(txn).key(key).value(message.getBytes()).send();
                    long end = System.currentTimeMillis();
                    System.err.println(
                            (end - start) + " --send ok" + key + " | " + send + "|" + producer.getLastDisconnectedTimestamp()
                                    + "|" + producer.getLastSequenceId()
                                    + "|" + producer.getTopic()
                                    + "|" + producer.getStats() + "--" + message);

                    Thread.sleep(1000);

                    if (i % 10 == 0) {
                        txn.commit();
                        System.err.println("commit -----------------");
                        txn = client.newTransaction().build().get();
                    }

                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }

// You can then send messages to the broker and topic you specified:

        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
