package com.hmdp.consumer;

import com.hmdp.dto.OrderMessage;
import com.hmdp.service.IVoucherOrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {
    private final IVoucherOrderService voucherOrderService;
    private final ObjectMapper objectMapper;

    public OrderConsumer(IVoucherOrderService voucherOrderService, ObjectMapper objectMapper) {
        this.voucherOrderService = voucherOrderService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order_topic", groupId = "order-group")
    public void consumeOrderMessage(String message) {
        try {
            OrderMessage orderMessage = objectMapper.readValue(message, OrderMessage.class);
            voucherOrderService.createVoucherOrder(orderMessage.getVoucherId(), orderMessage.getOrderId());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}