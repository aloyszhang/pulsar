package org.apache.pulsar;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.TopicStats;


public class PulsarAdminTest {
    public static void main(String[] args) throws Exception {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl("http://127.0.0.1:8081").build();


/*        String version = pulsarAdmin.brokers().getVersion();
        System.out.println(version);*/


        //System.out.println("version:" + version);

        String topic = "persistent://perf_test/perf_test/perf_test_8888-partition-0";

        MessageId lastMessageId = pulsarAdmin.topics().getLastMessageId(topic);
        System.out.println(lastMessageId);


        CompletableFuture<PartitionedTopicStats> overviewStats =
                pulsarAdmin.topics().getPartitionedOverviewStatsAsync(topic);
        System.out.println("overviewStats:" + overviewStats.get());

        List<String> subscriptions =
                pulsarAdmin.topics().getSubscriptions(topic);
        System.out.println("subscriptions:" + subscriptions);

        PartitionedTopicStats partitionedSubscriptionStats =
                pulsarAdmin.topics()
                        .getPartitionedSubscriptionStats(topic, subscriptions.stream().collect(Collectors.toSet()));
        System.out.println(partitionedSubscriptionStats);

        TopicStats a =
                pulsarAdmin.topics()
                        .getSubscriptionStats(topic + "-partition-0",
                                subscriptions.stream().collect(Collectors.toSet()));

        System.out.println(a);

    }


    public class VersionComparator {
        public static void main(String[] args) {
            String version1 = "1.2.3";
            String version2 = "1.2.4";

            // 将字符串版本号分割成数组，并转换成整数数组
            String[] parts1 = version1.split("\\.");
            String[] parts2 = version2.split("\\.");

            int[] numbers1 = new int[parts1.length];
            int[] numbers2 = new int[parts2.length];

            for (int i = 0; i < parts1.length; i++) {
                numbers1[i] = Integer.parseInt(parts1[i]);
            }
            for (int i = 0; i < parts2.length; i++) {
                numbers2[i] = Integer.parseInt(parts2[i]);
            }

            // 比较版本号
            for (int i = 0; i < Math.max(numbers1.length, numbers2.length); i++) {
                int num1 = i < numbers1.length ? numbers1[i] : 0;
                int num2 = i < numbers2.length ? numbers2[i] : 0;
                if (num1 < num2) {
                    System.out.println("version1 is older than version2");
                    return;
                } else if (num1 > num2) {
                    System.out.println("version1 is newer than version2");
                    return;
                }
            }
            System.out.println("version1 and version2 are the same");
        }
    }
}
