package org.apache.pulsar;

import java.net.InetAddress;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

public class PulsarConsumerNormalTest4 {

    public static void main(String[] args) {
        try {

            PulsarClient client =
                    PulsarClient.builder().serviceUrl("pulsar://localhost:6650").listenerThreads(2).build();

            String topic = "persistent://perf_test/perf_test/perf_test_8888";
            AtomicInteger count = new AtomicInteger();

            new Thread(() -> {
                for (; ; ) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    System.out.println("count" + count.get() + " time:" + new Date());
                    count.set(0);
                }

            }).start();

            Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(
                            SubscriptionInitialPosition.Latest)
                    .subscriptionName("subscription113")
                    .messageListener((MessageListener<byte[]>) (consumer1, msg) -> {
                        try {
                            consumer1.acknowledge(msg);
                            Thread.sleep(100);
                            count.incrementAndGet();

                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                    }).subscribe();

            Thread.sleep(3000);
            System.out.println(client);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
