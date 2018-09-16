package com.zookeeper.zkclient;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.List;

/**
 * 第一种watch机制，创建某个父节点的监听IZkChildListener，subscribeChildChanges()监听节点以及子节点的变化
 */
public class ZkWatchNode {

    private static final String CONNECTION_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final int SESSION_TIMEOUT = 3000;

    public static void main(String[] args) throws InterruptedException {
        ZkClient client = new ZkClient(new ZkConnection(CONNECTION_ADDR), SESSION_TIMEOUT);

        client.deleteRecursive("/super");

        // 在服务器链接过程中持续监听path子节点的变更,只是监听节点的变更，节点数据更新不监听
        List<String> list = client.subscribeChildChanges("/super", new IZkChildListener() {
            public void handleChildChange(String s, List<String> list) throws Exception {
                System.out.println("parent:" + s);
                System.out.println("child:" + list);
            }
        });
        System.out.println(list);

        Thread.sleep(1000);
        client.createPersistent("/super");
        Thread.sleep(1000);
        client.createPersistent("/super/c1","c1 Data");
        Thread.sleep(1000);
        client.createPersistent("/super/c2", "c2 Data");
        Thread.sleep(1000);
        client.writeData("/super","super");
        Thread.sleep(1000);
        // 删除节点需要跟根目录指定
        client.deleteRecursive("/super/c2");
        System.out.println(list);

        Thread.sleep(10000);
        client.close();
        System.out.println("zk断开链接");
    }
}
