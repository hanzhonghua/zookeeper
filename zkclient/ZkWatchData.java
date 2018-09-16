package com.zookeeper.zkclient;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

public class ZkWatchData {

    private static final String CONNECTION_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final int SESSION_TIMEOUT = 3000;

    public static void main(String[] args) throws InterruptedException {
        ZkClient zkClient = new ZkClient(new ZkConnection(CONNECTION_ADDR), SESSION_TIMEOUT);
        zkClient.subscribeDataChanges("/test2", new IZkDataListener() {
            public void handleDataChange(String s, Object o) throws Exception {
                System.out.println("节点："+s);
                System.out.println("数据："+o);
            }

            public void handleDataDeleted(String s) throws Exception {
                System.out.println("节点："+s);
            }
        });

        zkClient.createPersistent("/test2",true);
        zkClient.writeData("/test2","data");
        Thread.sleep(1000);
        zkClient.delete("/test2");
        Thread.sleep(1000);
    }
}
