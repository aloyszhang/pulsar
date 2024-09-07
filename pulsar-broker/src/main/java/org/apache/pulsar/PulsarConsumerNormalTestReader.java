package org.apache.pulsar;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

public class PulsarConsumerNormalTestReader {

    public static void main(String[] args) {
        try {

            System.out.println(InetAddress.getLocalHost().getHostName());

            PulsarClient client = PulsarClient.builder()
                    //.serviceUrl("http://100.76.43.216:8080")
                    .serviceUrl("pulsar://localhost:6650").connectionsPerBroker(10).ioThreads(10)
                    .build();

            String topic = "persistent://perf_test/perf_test/perf_test_8888";

            Reader<byte[]> reader =
                    client.newReader().topic(topic).subscriptionName("subscription113-reader")
                            .startMessageId(MessageId.latest).create();
            for (int i = 0; i < 10000000; i++) {
                Message<byte[]> message = reader.readNext();
                System.err.println(" consumer1 " + message.getKey()
                        + "| getRedeliveryCount | " + message.getRedeliveryCount() + "--|--" + message.getValue());
            }


            Thread.sleep(3000);
            System.out.println(client);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
