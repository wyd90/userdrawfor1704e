package com.bawei.flink.ordercalculate;

import com.bawei.flink.redisutil.JedisConnectionPool;
import redis.clients.jedis.Jedis;

import java.util.Set;

public class Test {
    public static void main(String[] args) {
        Jedis conn = JedisConnectionPool.getConnection();
        Set<String> keys = conn.keys("*");
        for(String key : keys) {
            String value = conn.get(key);
            System.out.println(key + "=" + value);
        }
        conn.close();
    }
}
