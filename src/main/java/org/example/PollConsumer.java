package org.example;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PollConsumer {
    private final Thread worker;
    private final DefaultLitePullConsumer consumer;
    // 拉取消息时间间隔 5秒
    private static final long POLL_INTERVAL_SECONDS = 5;

    public PollConsumer() throws MQClientException {
        // 实例化消费者
        this.consumer = new DefaultLitePullConsumer("please_rename_unique_group_name");
        this.consumer.setNamesrvAddr("localhost:9876");
        this.consumer.subscribe("TopicTest", "*");

        this.worker = new Thread(() ->{
            try {
                while (true) {
                    List<MessageExt> messageExts = consumer.poll();
                    System.out.printf("%d%s%n",messageExts.size(), messageExts.stream().map(MessageExt::getMsgId).collect(Collectors.toList()));
                    TimeUnit.SECONDS.sleep(POLL_INTERVAL_SECONDS); // todo best to use Scheduled
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                consumer.shutdown();
            }
        });
    }

    public void start() throws MQClientException {
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
        worker.start();
    }

}
