package com.hmdp.service.impl;

import com.hmdp.dto.OrderMessage;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.io.Serializable;
import java.util.Collections;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // @Resource
    // private RedissonClient redissonClient;

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Resource
    private RedisIdWorker redisIdWorker;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        // Long userId = UserHolder.getUser().getId();
        Long userId = 111L;
        long orderId = redisIdWorker.nextId("order");
//        // 1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString(), String.valueOf(orderId));
//        int r = result.intValue();
//        // 2.判断结果是否为0
//        if (r != 0) {
//            // 2.1.不为0 ，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
        // 发送订单消息到Kafka
        OrderMessage message = new OrderMessage();
        message.setOrderId(orderId);
        message.setUserId(userId);
        message.setVoucherId(voucherId);
        message.setTimestamp(System.currentTimeMillis());
        kafkaTemplate.send("order_topic", message.toString());

        // 3.返回订单id
        return Result.ok(orderId);
    }
    // @Override
    // public Result seckillVoucher(Long voucherId) {
    // // 1.查询优惠券
    // SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
    // // 2.判断秒杀是否开始
    // if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
    // // 尚未开始
    // return Result.fail("秒杀尚未开始！");
    // }
    // // 3.判断秒杀是否已经结束
    // if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
    // // 尚未开始
    // return Result.fail("秒杀已经结束！");
    // }
    // // 4.判断库存是否充足
    // if (voucher.getStock() < 1) {
    // // 库存不足
    // return Result.fail("库存不足！");
    // }
    // // 5.一人一单逻辑
    // Long userId = UserHolder.getUser().getId();
    // //创建锁对象(新增代码)
    // // redis实现的分布式锁
    // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId,
    // stringRedisTemplate);

    // 获取锁对象
    // boolean isLock = lock.tryLock(1200);
    // //加锁失败
    // if (!isLock) {
    // return Result.fail("不允许重复下单");
    // }
    // try {
    // //获取代理对象(事务)
    // IVoucherOrderService proxy = (IVoucherOrderService)
    // AopContext.currentProxy();
    // return proxy.createVoucherOrder(voucherId);
    // } finally {
    // //释放锁
    // lock.unlock();
    // }
    // }

    @Transactional
    public Result createVoucherOrder(Long voucherId, Long orderId) {
        // 5.1.用户id
        // Long userId = UserHolder.getUser().getId();
        Long userId = 1L;
//        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        // 5.2.判断是否存在
//        if (count > 0) {
//            // 用户已经购买过了
//            return Result.fail("用户已经购买过一次！");
//        }

        // 6，扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock= stock -1")
//                .eq("voucher_id", voucherId).update();
//        if (!success) {
//            // 扣减库存
//            return Result.fail("库存不足！");
//        }
        // 7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 7.1.订单id
        voucherOrder.setId(orderId);

        voucherOrder.setUserId(userId);
        // 7.3.代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        return Result.ok(orderId);
    }
}
