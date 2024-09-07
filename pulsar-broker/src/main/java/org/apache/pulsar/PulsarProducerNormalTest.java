package org.apache.pulsar;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

public class PulsarProducerNormalTest {

    public static void main(String[] args) {
        try {

            System.setProperty("io.netty.hostsFileRefreshInterval", "1000");

            System.out.println(InetAddress.getLocalHost().getHostName());

            PulsarClient client = PulsarClient.builder()
                    .enableTransaction(true)
                    .serviceUrl("pulsar://test.pulsar.com:6651").connectionsPerBroker(1)
                    .ioThreads(1)
                    .operationTimeout(5, TimeUnit.MINUTES)
                    .build();

            String topic = "persistent://perf_test/perf_test/perf_test_8888";

            Producer<byte[]> producer = client.newProducer()
                    .topic(topic)
                    .create();


            // Transaction transaction = client.newTransaction().build().get();

            System.out.println("start");

            for (int i = 0; i < 100000; i++) {
                try {

                    String message = "My message" + i + " | " + System.currentTimeMillis();
                    long start = System.currentTimeMillis();
                    MessageId send =
                            producer.newMessage().key(String.valueOf(i)).value(message.getBytes())
                                    .send();
                    long end = System.currentTimeMillis();
                    System.err.println(
                            (end - start) + " --send ok" + send + "|" + producer.getLastDisconnectedTimestamp()
                                    + "|" + producer.getLastSequenceId()
                                    + "|" + producer.getTopic()
                                    + "|" + producer.getStats() + "--" + message);

                    Thread.sleep(1);

                    if (i % 10 == 0) {
                        // transaction.commit();
                        System.out.println("commit ");
                        // transaction = client.newTransaction().build().get();
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
