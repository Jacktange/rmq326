package com.alibaba.rocketmq.client.quickstart;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {

	public static void main(String[] args) throws Exception {

		String producerGroup = "quickstart_producer";
		String namesrvAddr = "192.168.88.49:9876";
		DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
		producer.setNamesrvAddr(namesrvAddr);

		int retryTimesWhenSendFailed = 10;// 10
		producer.setRetryTimesWhenSendFailed(retryTimesWhenSendFailed);

		producer.start();

		String topic = "TopicQuickStart";
		String tags = "TagA";

		for (int i = 0; i < 10; i++) {
			String body = "Hello RocketMQ " + i;
			Message message = new Message(topic, tags, body.getBytes());
			SendResult sendResult = producer.send(message);
			// SendResult sendResult = producer.send(message,1000);
			System.out.println("sendResult: " + sendResult);
		}

		producer.shutdown();

	}

}
