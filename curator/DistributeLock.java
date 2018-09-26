package com.zookeeper.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class DistributeLock {

    private static final String CONNECTION_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final int SESSION_TIMEOUT = 5000;

    static int count = 10;
    public static void genarNo(){
        count--;
        System.out.println(count);
    }

    public static void main(String[] args) throws InterruptedException {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework curator = CuratorFrameworkFactory.builder().connectString(CONNECTION_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(retry).namespace("super").build();
        curator.start();

        // 分布式锁
        final InterProcessMutex lock = new InterProcessMutex(curator, "/super");
        for (int a=0; a<10; a++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        // 加锁
                        lock.acquire();
                        genarNo();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }finally {
                        try {
                            lock.release();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }, "t" + a).start();
        }
        Thread.sleep(1000);
        // curator.close();
    }
}
