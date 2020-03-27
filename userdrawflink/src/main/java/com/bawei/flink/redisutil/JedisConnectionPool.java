package com.bawei.flink.redisutil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;

public class JedisConnectionPool {
    private static JedisPool jedisPool = null;
    private static JedisPoolConfig config = null;

    static {
        config = new JedisPoolConfig();
        //最大连接数
        config.setMaxTotal(10);
        //最大空闲连接数
        config.setMaxIdle(5);
        //进行取值时检查有效性
        config.setTestOnBorrow(true);
    }

    public static Jedis getConnection() {
        if(jedisPool == null) {
            jedisPool = new JedisPool(config,"192.168.56.104",6379,10000,null);
        }
        return jedisPool.getResource();
    }


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
