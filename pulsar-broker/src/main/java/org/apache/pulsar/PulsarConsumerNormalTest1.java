package org.apache.pulsar;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

public class PulsarConsumerNormalTest1 {

    public static void main(String[] args) {
        try {

            System.out.println(InetAddress.getLocalHost().getHostName());

            PulsarClient client = PulsarClient.builder()
                    //.serviceUrl("http://100.76.43.216:8080")
                    .serviceUrl("pulsar://localhost:6650").connectionsPerBroker(10).ioThreads(10)
                    .build();

            String topic = "persistent://perf_test/perf_test/perf_test_8888";

            Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(
                            SubscriptionInitialPosition.Latest)
                    .subscriptionName("subscription114").enableBatchIndexAcknowledgment(true)
                    .enableBatchIndexAcknowledgment(true)
                    .replicateSubscriptionState(true).ackTimeout(5, TimeUnit.SECONDS)
                    .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build())
                    .messageListener((MessageListener<byte[]>) (consumer1, msg) -> {

                        try {
                            System.err.println(consumer1.getConsumerName() + " consumer1 " + msg.getKey()
                                    + "| getRedeliveryCount | " + msg.getRedeliveryCount() + "--|--" + msg.getValue());
                            consumer1.acknowledge(msg);

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
