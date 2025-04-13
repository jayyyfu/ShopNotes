package com.hmdp.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hmdp.dto.OrderMessage;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.RetryOrderService;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class OrderConsumer {
    @Autowired
    private RetryOrderService retryOrderService;
    private final IVoucherOrderService voucherOrderService;
    private final ObjectMapper objectMapper;
    private final ThreadPoolExecutor orderExecutor = new ThreadPoolExecutor(
        10, // 核心线程数：保持常驻线程应对常规订单量
        15, // 最大线程数：突发流量时弹性扩容
        60L, TimeUnit.SECONDS, // 线程空闲超时时间
        new ArrayBlockingQueue<>(100), // 有界队列防止内存溢出
        Executors.defaultThreadFactory(),
        new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略：调用方线程执行保证消息不丢失
    );

    public OrderConsumer(IVoucherOrderService voucherOrderService, ObjectMapper objectMapper) {
        this.voucherOrderService = voucherOrderService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order_topic", groupId = "order-group")
    public void consumeOrderMessage(String message, Acknowledgment ack) {
        try {
            OrderMessage orderMessage = objectMapper.readValue(message, OrderMessage.class);
            orderExecutor.execute(() -> {
                try {
                    voucherOrderService.createVoucherOrder(orderMessage.getVoucherId(), orderMessage.getOrderId());
                    ack.acknowledge();
                } catch (Exception e) {
                    log.error("订单创建失败 voucherId:{}, orderId:{}", orderMessage.getVoucherId(), orderMessage.getOrderId(), e);
                    // 将失败消息放入重试队列
                    retryOrderService.sendToRetryQueue(orderMessage);
                }
            });
        } catch (Exception e) {
            log.error("消息反序列化失败", e);
            ack.acknowledge(); // 无效消息直接确认
        }
    }

    @Retryable(maxAttempts = 4, backoff = @Backoff(delay = 3000, multiplier = 2))
    @KafkaListener(topics = "retry_order_topic", groupId = "order-group")
    public void retryOrderMessage(String message, Acknowledgment ack) throws JsonProcessingException {
        try {
            OrderMessage orderMessage = objectMapper.readValue(message, OrderMessage.class);
            
            // TODO 幂等性校验

            voucherOrderService.createVoucherOrder(orderMessage.getVoucherId(), orderMessage.getOrderId());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("重试处理失败 重试次数:{}", RetrySynchronizationManager.getContext().getRetryCount(), e);
            throw e;
        }
    }

    @Recover
    private void recoverRetry(Throwable t, String message, Acknowledgment ack) {
        try {
            OrderMessage orderMessage = objectMapper.readValue(message, OrderMessage.class);
            log.error("达到最大重试次数，转入死信队列 voucherId:{}, orderId:{}", 
                orderMessage.getVoucherId(), orderMessage.getOrderId());
            retryOrderService.sendToDlq(orderMessage);
        } catch (Exception ex) {
            log.error("死信队列处理异常", ex);
        } finally {
            ack.acknowledge();
        }
    }
}