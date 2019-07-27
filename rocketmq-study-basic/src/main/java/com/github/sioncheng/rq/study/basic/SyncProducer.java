package com.github.sioncheng.rq.study.basic;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class SyncProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("rq-study-basic");
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        defaultMQProducer.start();

        Message message = new Message("topic-1",  "message content".getBytes("UTF-8"));

        SendResult sendResult = defaultMQProducer.send(message);

        defaultMQProducer.shutdown();
    }
}
