package me.wuzhaoyang.idhappiness;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by john on 17/7/13.
 * Id生成器DEMO实现，使用Snowflake算法</br>
 * 可参见<a href="http://wuzhaoyang.me/2017/07/12/distributed-idgen.html">分布式ID生成器</a></br>
 * －－－－－－－－－－－－－－－－－－－－－－－－－－－－－－---
 * |  1   |   39    ｜  2    ｜    7      ｜   7  ｜   8  ｜
 * －－－－－－－－－－－－－－－－－－－－－－－－－－－－－－---
 * |  0   | 时间戳   |  机房   |   机器号   | 业务号 | 序列号｜
 * －－－－－－－－－－－－－－－－－－－－－－－－－－－－－－---
 * 分析：
 * <ul>
 * <li>计划使用10年，10*365*24*60*60*1000ms ～ 2^39，需要39bit时间戳</li>
 * <li>5年内不超过4个机房，需要2bit</li>
 * <li>每个机房不超过100台服务器，需要7bit</li>
 * <li>业务线不超过100条，需要7bit</li>
 * <li>tps100W/s，并发不超过100/ms,7bit已足够，这里就用完8bit</li>
 * </ul>
 */
public class Idgen {

    //基准时间：2017年7月13号
    private static final long baseTimeStamp = 1499958713647L;

    //序列号占用位数
    private static final long sequenceBits = 8L;
    //业务号占用位数
    private static final long businessIdBits = 7L;
    //机器号占用位数
    private static final long workIdBits = 7L;
    //机房号占用尾数
    private static final long regionIdBits = 2L;

    //最大数
    private static final long maxRegionId = ~(-1L << regionIdBits);
    private static final long maxWorkId = ~(-1L << workIdBits);
    private static final long maxBusinessId = ~(-1L << businessIdBits);
    private static final long maxSequence = ~(-1L << sequenceBits);

    //offset
    private static final long timeOffset = sequenceBits + businessIdBits + workIdBits + regionIdBits;
    private static final long regionOffset = sequenceBits + businessIdBits + workIdBits;
    private static final long workIdOffset = sequenceBits + businessIdBits;
    private static final long businessIdOffset = sequenceBits;

    //机房号
    private long regionId;
    //机器号
    private long workId;
    //业务号
    private long bizId;
    //序列号
    private AtomicLong sequenceId;

    //上次发号的毫秒数
    private long lastTimeMillis = -1L;

    public Idgen(long regionId, long workId, long bizId) throws Exception {
        if (regionId > maxRegionId || regionId < 0)
            throw new Exception("机房号超出范围");
        if (workId > maxWorkId || workId < 0)
            throw new Exception("机器号超出范围");
        if (bizId > maxBusinessId || bizId < 0)
            throw new Exception("业务号超出范围");
        this.regionId = regionId;
        this.workId = workId;
        this.bizId = bizId;
    }

    public synchronized Long getNext() throws Exception {
        long currentTimeMillis = timeGen();
        if (currentTimeMillis < lastTimeMillis) {
            throw new Exception("异常，发生时钟回拨");
        }
        //还在当前毫秒数
        if (currentTimeMillis == lastTimeMillis) {
            long newSequenceId = sequenceId.incrementAndGet() & maxSequence;
            //8bit的序列号在当前毫秒用完
            if (newSequenceId == 0L) {
                //自旋等待
                currentTimeMillis = waitUtilNextMillis();
                sequenceId = getRandomSeq();
            }
        } else {//下一毫秒开始
            sequenceId = getRandomSeq();

        }
        lastTimeMillis = currentTimeMillis;

        return (currentTimeMillis - baseTimeStamp) << timeOffset | regionId << regionOffset | workId << workIdOffset | bizId << businessIdOffset | sequenceId.get();
    }

    //均匀尾数
    private AtomicLong getRandomSeq() {
        //System.out.println("新毫秒的尾数");
        return new AtomicLong(new SecureRandom().nextInt(10));

    }

    private long waitUtilNextMillis() {
        long time = timeGen();
        while (time <= lastTimeMillis) {
            //System.out.println("wait lastTimeMillis=" + lastTimeMillis);
            time = timeGen();
        }
        return time;
    }

    private long timeGen() {
        return Clock.systemUTC().millis();
    }

    public static void main(String[] args) throws Exception {
        Idgen idgen = new Idgen(1L, 1L, 2L);
        AtomicLong atomicLong = new AtomicLong(0);
        AtomicLong dump = new AtomicLong(0);
        long oneSecond = Clock.systemUTC().millis() + 1000L;
        Set<Long> set = new HashSet<>();
        while (Clock.systemUTC().millis() <= oneSecond) {
            Long next = idgen.getNext();
            if (set.contains(next)) {
                dump.incrementAndGet();
            } else {
                set.add(next);
            }
            atomicLong.incrementAndGet();
        }
        System.out.println("1s内一共生成" + atomicLong.get() + "个Id,其中重复" + dump.get() + "个Id");
    }
}
