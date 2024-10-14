package org.apache.pulsar;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;

public class PulsarConsumerNormalTest {

    public static void main(String[] args) {
        try {

            System.out.println(InetAddress.getLocalHost().getHostName());

            PulsarClient client = PulsarClient.builder()
                    //.serviceUrl("http://100.76.43.216:8080")
                    .serviceUrl("pulsar://localhost:6650").connectionsPerBroker(10).ioThreads(10).listenerThreads(100)
                    .build();

            String topic = "persistent://compaction/compaction/compaction_test-partition-0";

            Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(
                            SubscriptionInitialPosition.Latest)
                    .subscriptionName("test").enableBatchIndexAcknowledgment(true)
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

            ConsumerImpl consumer1 = (ConsumerImpl) consumer;


            for (int i = 0; i < 1000; i++) {
                long start = System.currentTimeMillis();
                Object a = consumer1.internalGetLastMessageIdAsync().get();
                Field lastMessageId = a.getClass().getDeclaredField("lastMessageId");
                lastMessageId.setAccessible(true);
                MessageId messageId = (MessageId) lastMessageId.get(a);
                System.out.println(messageId + " cost: " + (System.currentTimeMillis() - start));
                Thread.sleep(1000);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
