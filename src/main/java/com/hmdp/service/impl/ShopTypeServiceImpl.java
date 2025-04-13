package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_TYPE_LIST;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryForList() {
        //1. 从redis查询商铺类型
        List<String> shopTypeList = stringRedisTemplate.opsForList().range(CACHE_TYPE_LIST, 0, -1);

        //2. 判断是否存在
        if (!shopTypeList.isEmpty()) {
            //3. 存在就返回
            List<ShopType> typeList = new ArrayList<>();
            for (String s : shopTypeList) {
                ShopType shopType = JSONUtil.toBean(s, ShopType.class);
                typeList.add(shopType);
            }
            return Result.ok(typeList);
        }

        //4.没中数据库中查
        List<ShopType> typeList = query().orderByAsc("sort").list();

        //5.不存在直接返回错误
        if (typeList.isEmpty()) {
            return Result.fail("分类不存在!");
        }

        //6.存在，写入redis
        for (ShopType shopType : typeList) {
            String s = JSONUtil.toJsonStr(shopType);

            shopTypeList.add(s);
        }
        stringRedisTemplate.opsForList().rightPushAll(CACHE_TYPE_LIST, shopTypeList);

        return Result.ok(typeList);
    }
}
