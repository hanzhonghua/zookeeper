package com.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Curator框架，支持链式编程
 */
public class CuratorBase {

    private static final String CONNECTION_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final int SESSION_TIMEOUT = 3000;

    public static void main(String[] args) throws Exception {

        // 重试策略，初始时间间隔1s，重试10次
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework build = CuratorFrameworkFactory.builder().connectString(CONNECTION_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT).retryPolicy(retry).build();
        build.start();
       // build.delete().deletingChildrenIfNeeded().forPath("/super");

       // build.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/super/c1", "test c1".getBytes());
       // build.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/super/c2", "test c2".getBytes());

        // 遍历子节点
        /*List<String> list = build.getChildren().forPath("/super");
        for (String path : list){
            System.out.println("/super子节点：" + path);
        }*/

        // 查询节点数据
        /*build.setData().forPath("/super", "super Data".getBytes());
        byte[] bytes = build.getData().forPath("/super");
        System.out.println("/super节点数据：" + new String(bytes));*/

        // 回调,新增修改删除的时候回调函数，做一些事情，支持使用线程池的
        ExecutorService pool = Executors.newCachedThreadPool();
        build.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("code:" + curatorEvent.getResultCode());
                        System.out.println("type:" + curatorEvent.getType());
                        System.out.println("线程：" + Thread.currentThread().getName());
                    }
                }, pool).forPath("/super/c3","test c3".getBytes());
        System.out.println("主线程：" + Thread.currentThread().getName());

        Thread.sleep(Integer.MAX_VALUE);
        build.close();
    }
}
