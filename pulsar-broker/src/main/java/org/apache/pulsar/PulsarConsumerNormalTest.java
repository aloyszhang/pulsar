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

            String topic = "perf_test/perf_test/last_message_test-partition-0";

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

            Object a = consumer1.internalGetLastMessageIdAsync().get();

            Field lastMessageId = a.getClass().getDeclaredField("lastMessageId");
            lastMessageId.setAccessible(true);
            MessageId messageId = (MessageId)lastMessageId.get(a);
            System.out.println(messageId);


            System.out.println(a);




            Thread.sleep(3000);
            System.out.println(client);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
