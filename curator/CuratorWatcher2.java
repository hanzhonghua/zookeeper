package com.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorWatcher2 {

    private static final String CONNECTION_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final int SESSION_TIMEOUT = 3000;

    public static void main(String[] args) throws Exception {

        // 重试策略，初始时间间隔1s，重试10次
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework build = CuratorFrameworkFactory.builder().connectString(CONNECTION_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT).retryPolicy(retry).build();
        build.start();

        // 第三个参数是否接受节点变更时的内容，需要设置为true，如果设置为false时更新数据时将不监听，正常情况下需设置true
        // 监听节点数据变更，子节点的创建删除以及节点数据变更，不会监听节点自己的删除
        PathChildrenCache cache = new PathChildrenCache(build, "/super", true);
        // 设置缓存监听
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                switch (pathChildrenCacheEvent.getType()){
                    case CHILD_ADDED:
                        System.out.println("CHILD_ADDED path：" + pathChildrenCacheEvent.getData().getPath());
                        System.out.println("CHILD_ADDED data：" + new String(pathChildrenCacheEvent.getData().getData()));
                    case CHILD_UPDATED:
                        System.out.println("CHILD_UPDATED：" + pathChildrenCacheEvent.getData().getPath());
                        System.out.println("CHILD_UPDATED data：" + new String(pathChildrenCacheEvent.getData().getData()));
                    case CHILD_REMOVED:
                        System.out.println("CHILD_REMOVED：" + pathChildrenCacheEvent.getData().getPath());
                        System.out.println("CHILD_REMOVED data：" + new String(pathChildrenCacheEvent.getData().getData()));
                }
            }
        });

        Thread.sleep(1000);
        build.create().forPath("/super", "super".getBytes());
        Thread.sleep(1000);
        build.setData().forPath("/super","new super".getBytes());
        Thread.sleep(1000);
        build.create().forPath("/super/c1", "c1 data".getBytes());
        Thread.sleep(1000);
        build.create().forPath("/super/c2", "c2 data".getBytes());
        Thread.sleep(1000);
        build.setData().forPath("/super/c2", "new c2 data".getBytes());
        Thread.sleep(1000);
        build.delete().forPath("/super/c2");
        build.delete().deletingChildrenIfNeeded().forPath("/super");
        Thread.sleep(Integer.MAX_VALUE);
    }
}
