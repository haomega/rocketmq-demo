package org.example;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 生成消息 1条/s
 */
public class Producer {
    private final Thread worker;
    private final AtomicInteger atomicInteger;
    // 实例化消息生产者Producer
    private final DefaultMQProducer producer ;

    public Producer() {
        this.worker = new Thread(this::run);
        // 实例化消息生产者Producer
        this.producer = new DefaultMQProducer("please_rename_unique_group_name");
        this.atomicInteger = new AtomicInteger(0);
    }

    public void start() throws MQClientException {
        // 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动Producer实例
        producer.start();

        worker.start();
    }

    // 发送同步消息
    private void run() {
        while (true) {
            try {
                // 创建消息，并指定Topic，Tag和消息体
                Message msg = new Message("TopicTest" /* Topic */,
                        "TagA" /* Tag */,
                        ("Hello RocketMQ " + atomicInteger.getAndIncrement()).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );
                // 发送消息到一个Broker
                SendResult sendResult = producer.send(msg);
                // 通过sendResult返回消息是否成功送达
                System.out.println("send success! " + sendResult.getMsgId());
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (Exception e) {
                System.out.println("send error!" + e);
            }
        }
    }
}
