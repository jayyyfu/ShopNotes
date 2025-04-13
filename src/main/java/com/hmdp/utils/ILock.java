package com.hmdp.utils;

public interface ILock {

    /**
     * 尝试获取锁
     * @param timeoutSec 锁持有的超时时间，过期自动释放
     * @return
     */
    boolean tryLock(long timeoutSec);

    void unlock();
}
