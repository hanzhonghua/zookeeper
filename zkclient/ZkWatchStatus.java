package com.zookeeper.zkclient;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.Watcher;

import java.util.List;

/**
 * zk服务状态监听
 */
public class ZkWatchStatus {

    private static final String CONNECTION_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final int SESSION_TIMEOUT = 3000;

    public static void main(String[] args) throws InterruptedException {
        ZkClient client = new ZkClient(new ZkConnection(CONNECTION_ADDR), SESSION_TIMEOUT);

        client.deleteRecursive("/super");

        // 在服务器链接过程中持续监听path子节点的变更,只是监听节点的变更，节点数据更新不监听
        client.subscribeStateChanges(new IZkStateListener() {
            public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
                if(keeperState == Watcher.Event.KeeperState.SyncConnected){
                    System.out.println("连接成功");
                }else if(keeperState== Watcher.Event.KeeperState.Disconnected){
                    System.out.println("断开链接");
                }else if(keeperState==Watcher.Event.KeeperState.Expired){
                    System.out.println("会话失效");
                }else if(keeperState==Watcher.Event.KeeperState.AuthFailed){
                    System.out.println("权限认证失败");
                }
            }

            public void handleNewSession() throws Exception {

            }

            public void handleSessionEstablishmentError(Throwable throwable) throws Exception {

            }
        });

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

        Thread.sleep(1000);
        //client.close();
        System.out.println("zk断开链接");
    }
}
