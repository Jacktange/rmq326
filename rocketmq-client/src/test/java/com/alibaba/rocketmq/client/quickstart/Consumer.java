package com.alibaba.rocketmq.client.quickstart;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer {

	public static void main(String[] args) throws Exception {

		String consumerGroup = "quickstart_consumer";
		String namesrvAddr = "192.168.88.49:9876";
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
		consumer.setNamesrvAddr(namesrvAddr);
		/**
		 * 设置Consumer第一次启动是从队列头部开始消费 还是队列尾部开始消费 如果非第一次启动 那么按照 上次消费的位置继续消费
		 */
		ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
		consumer.setConsumeFromWhere(consumeFromWhere);

		// 批量消费 一次消费多少条消息 默认一条 consumeMessageBatchMaxSize
		int consumeMessageBatchMaxSize = 10;
		consumer.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize);

		String topic = "TopicQuickStart";
		String subExpression = "*";
		// 订阅
		consumer.subscribe(topic, subExpression);
		// 批量拉取消息 一次最多拉多少条 默认32条
		// DefaultMQPushConsumer使用setPullBatchSize 不好用
		// int pullBatchSize = 32;
		// consumer.setPullBatchSize(pullBatchSize);

		// 当前消息监听器
		// 虽然写的 For循环 消息实际是一条一条接收的
		consumer.registerMessageListener(new MessageListenerConcurrently() {

			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				// System.out.println(Thread.currentThread().getName()+" Receive New Messages:
				// "+msgs);
				String topic = "topic";
				String tags = "tags";
				String msgBody = "msgBody";

				System.out.println("消息条数： " + msgs.size());
				MessageExt message = msgs.get(0);

				try {
					/*
					 * for(MessageExt msg : msgs){ topic = msg.getTopic(); tags = msg.getTags();
					 * msgBody = new String(msg.getBody(),"utf-8");
					 * System.out.println("收到消息： "+" Topic: "+topic+" Tags: "+tags+" MsgBody: "
					 * +msgBody);
					 * 
					 * // 一定要注意先启动Consumer,在进行发送消息(先定义,再发送) if("Hello RocketMQ 4".equals(msgBody)){
					 * System.out.println("=================失败消息开始===================");
					 * System.out.println(msg); System.out.println("Message Body: "+msgBody);
					 * System.out.println("=================失败消息结束===================");
					 * 
					 * // 抛出异常 //int a = 1/0; } }
					 */

					topic = message.getTopic();
					tags = message.getTags();
					msgBody = new String(message.getBody(), "utf-8");
					System.out.println("收到消息： " + " Topic: " + topic + " Tags: " + tags + " MsgBody: " + msgBody);

					// 一定要注意先启动Consumer,在进行发送消息(先定义,再发送)
					if ("Hello RocketMQ 4".equals(msgBody)) {
						System.out.println("=================失败消息开始===================");
						System.out.println(message);
						System.out.println("Message Body: " + msgBody);
						System.out.println("=================失败消息结束===================");
						// 抛出异常
						int a = 1 / 0;
					}

				} catch (Exception e) {
					/*
					 * e.printStackTrace();// 稍后发送 return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					 */
					e.printStackTrace();
					if (message.getReconsumeTimes() >= 2) {// 重试两次以后
						// 操作数据库 记录失败消息日志
						// To-Do
						System.out.println("重试指定次数后,开始写入数据库失败日志......");
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					} else {
						return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					}
				}

				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		consumer.start();

		System.out.println("Consumer Started.");

	}

}
