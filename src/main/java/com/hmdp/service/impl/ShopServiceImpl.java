package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPashThrough(id);
        Shop shop = cacheClient.queryWithPashThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        //(需要先预热)
        //Shop shop = queryWithLogicalExpire(id);
        //Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }

        return Result.ok(shop);
    }

    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;

        //1. 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2. 判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3. 存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //防止缓存穿透：判断命中的是否为空值
        if (Objects.equals(shopJson, "")) {
            return null;
        }

        //4. 实现缓存重建
        //4.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = trylock(lockKey);

            //4.2 判断是否获取成功
            if (!isLock) {
                //4.3 失败， 则休眠并重试
                Thread.sleep(200);
                return queryWithMutex(id);
            }

            //4. 获取锁成功， 根据id查询数据库
            shop = getById(id);

            //5. 数据库中不存在，返回错误
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }

            //6. 存在，写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(lockKey);
        }

        return shop;
    }

//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

//    public Shop queryWithLogicalExpire(Long id){
//        String key = CACHE_SHOP_KEY + id;
//
//        //1. 从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //2. 判断是否存在
//        if (StrUtil.isBlank(shopJson)) {
//            //3. 不存在，返回null
//            return null;
//        }
//
//        //4. 存在， 需要先把json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//
//        //5. 判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            //5.1 未过期，直接返回店铺信息
//            return shop;
//        }
//
//        //5.2. 已过期，需要重建缓存
//        //6. 缓存重建
//        //6.1. 获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = trylock(lockKey);
//        //6.2. 判断是否获取成功
//        if (isLock) {
//            // TODO 这里需要DoubleCheck
//
//            //6.3. 成功，开启独立线程，实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    saveshop2Redis(id, 20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    unlock(lockKey);
//                }
//            });
//
//        }
//
//        //返回过期的商铺信息
//        return shop;
//    }

//    public Shop queryWithPashThrough(Long id){
//        String key = CACHE_SHOP_KEY + id;
//
//        //1. 从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //2. 判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            //3. 存在，直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//
//        //防止缓存穿透：判断命中的是否为空值
//        if (Objects.equals(shopJson, "")) {
//            //返回一个错误信息
//            return null;
//        }
//
//        //4. 不存在，根据id查询数据库
//        Shop shop = getById(id);
//
//        //5. 数据库中不存在，返回错误
//        if (shop == null) {
//            //将空值写入redis
//            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//            //返回错误信息
//            return null;
//        }
//
//        //6. 存在，写入redis
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        return shop;
//    }

    private boolean trylock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

//    private void saveshop2Redis(Long id, Long expireSeconds) {
//        //11.查询店铺数据
//        Shop shop = getById(id);
//        //2. 封装逻辑过期时间
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//        //3.写入Redis
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
//    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空！");
        }
        //更新数据库
        updateById(shop);

        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
