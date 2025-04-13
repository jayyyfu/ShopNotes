package com.hmdp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.consumer.OrderConsumer;
import com.hmdp.dto.OrderMessage;
import com.hmdp.dto.Result;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.concurrent.ListenableFuture;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
@ActiveProfiles("test")
public class VoucherOrderServiceIntegrationTest {

    @Autowired
    private IVoucherOrderService voucherOrderService;

    @Autowired
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    private OrderConsumer orderConsumer;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private Long voucherId;
    private Long userId = 111L;

    @Autowired
    private ObjectMapper objectMapper; // 注入ObjectMapper

    @BeforeEach
    void setup() {
        // 初始化测试用秒杀券（需要先实现秒杀券创建逻辑）
        voucherId = 1001L;
        // 设置库存为1进行测试(已手动添加)
        // seckillVoucherService.setStock(voucherId, 1);
    }

    @Test
    void testNormalOrder() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // 正常下单测试
        Result result = voucherOrderService.seckillVoucher(voucherId);
        System.out.println(result);
        //assertEquals(200, result.getCode());
        assertNotNull(result.getData());
        Long orderId = (long) result.getData();
        // 验证消息发送
        try {
            OrderMessage message = new OrderMessage();
            message.setOrderId(orderId);
            message.setUserId(userId);
            message.setVoucherId(voucherId);
            message.setTimestamp(System.currentTimeMillis());
            String jsonMessage = objectMapper.writeValueAsString(message); // 转为JSON
            kafkaTemplate.send("order_topic", jsonMessage);
            //kafkaTemplate.send("order_topic", message.toString());
            //assertFalse(future.get(2, TimeUnit.SECONDS).getProducerRecord().value().isEmpty());
        } catch (Exception e) {
            fail("消息发送失败: " + e.getMessage());
        }

        // 4. 轮询检查订单是否处理完成（或使用CountDownLatch）
        boolean orderProcessed = false;
        long maxWaitTime = 5000; // 最大等待5秒
        while (System.currentTimeMillis() - startTime < maxWaitTime) {
            if (voucherOrderService.getById(orderId) != null) {
                orderProcessed = true;
                break;
            }
            Thread.sleep(50); // 每200ms检查一次
        }

        // 5. 计算总耗时
        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println("异步下单总耗时: " + totalTime + "ms");

        // 6. 验证结果
        assertTrue(orderProcessed, "订单未在预期时间内处理完成");
        assertTrue(totalTime < 3000, "处理时间超过3秒阈值"); // 根据业务需求调整阈值
        // 等待消费者处理
        // Thread.sleep(3000);

        // Long orderId = (long) result.getData();
        // 验证数据库订单
        assertNotNull(voucherOrderService.getById(orderId));
    }

//     @Test
//     void testInsufficientStock() {
//         // 库存不足测试
//         // 先消耗库存
//         voucherOrderService.seckillVoucher(voucherId);
//         // 再次尝试下单
//         Result result = voucherOrderService.seckillVoucher(voucherId);
//         assertEquals("库存不足", result.getMsg());
//     }

//     @Test
//     void testDuplicateOrder() {
//         // 重复下单测试
//         voucherOrderService.seckillVoucher(voucherId);
//         Result result = voucherOrderService.seckillVoucher(voucherId);
//         assertEquals("不能重复下单", result.getMsg());
//     }
}