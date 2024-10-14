package org.apache.pulsar;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;

public class PulsarProducerTxnTest {

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

            for (int i = 0; i < 1000; i++) {

                int finalI = i;
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Producer<byte[]> producer = client.newProducer()
                                    .topic(topic)
                                    .create();

                            Transaction txn = client.newTransaction().build().get();

                            System.out.println("start");
                            String key = "C";
                            if (System.currentTimeMillis() % 2 == 0) {
                                key = "D";
                            }

                            String message = "My message" + finalI + " | " + System.currentTimeMillis();
                            long start = System.currentTimeMillis();
                            MessageId send = producer.newMessage(txn).key(key).value(message.getBytes()).send();
                            long end = System.currentTimeMillis();
                            System.err.println(
                                    (end - start) + " --send ok" + key + " | " + send + "|"
                                            + producer.getLastDisconnectedTimestamp()
                                            + "|" + producer.getLastSequenceId()
                                            + "|" + producer.getTopic()
                                            + "|" + producer.getStats() + "--" + message);

                            //txn.commit();
                        } catch (Throwable throwable) {
                            throwable.printStackTrace();
                        }
                    }
                }).start();
            }
// You can then send messages to the broker and topic you specified:

        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
