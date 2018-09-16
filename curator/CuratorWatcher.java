package com.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorWatcher {

    private static final String CONNECTION_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final int SESSION_TIMEOUT = 3000;

    public static void main(String[] args) throws Exception {

        // 重试策略，初始时间间隔1s，重试10次
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework build = CuratorFrameworkFactory.builder().connectString(CONNECTION_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT).retryPolicy(retry).build();
        build.start();

        // 建立cache缓存,第三个参数为是否压缩，只是针对设置的一个节点，这种方式不太常用
        final NodeCache cache = new NodeCache(build, "/super", false);
        cache.start(true);
        cache.getListenable().addListener(new NodeCacheListener() {
            /**
             * 监听节点的创建，数据更新，但是不监听节点删除
             * @throws Exception
             */
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("路径：" + cache.getCurrentData().getPath());
                System.out.println("数据：" + new String(cache.getCurrentData().getData()));
                System.out.println("状态：" + cache.getCurrentData().getStat());
                System.out.println("----------------------");
            }
        });

        Thread.sleep(1000);
        build.create().forPath("/super", "super".getBytes());
        Thread.sleep(1000);
        build.setData().forPath("/super","new super".getBytes());
        Thread.sleep(1000);
        build.delete().forPath("/super");
        Thread.sleep(Integer.MAX_VALUE);
    }
}
