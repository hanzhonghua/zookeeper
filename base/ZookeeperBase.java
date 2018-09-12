package com.zookeeper.base;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperBase {

    private static final String CONNECT_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final int SESSION_OUTTIME = 5000;
    private static final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME, new Watcher() {
            public void process(WatchedEvent event) {
                // 事件状态
                Event.KeeperState state = event.getState();
                // 事件类型
                Event.EventType type = event.getType();
                // 如果是建立连接
                if (Event.KeeperState.SyncConnected == state) {
                    // None表示刚开始，没有什么状态
                    if (Event.EventType.None == type) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        /**
                         *  zk客户端和服务端建立连接时异步的，所以使用这种方式直到服务端返回建立连接才继续向西执行
                         *  如果连接建立成功，释放阻塞线程继续执行
                         */
                        latch.countDown();

                        System.out.println("zk 已建立连接");
                    }
                }
            }
        });

        latch.await();
        System.out.println("继续执行");
        /**
         *  参数1：节点名称,不允许递归创建节点，如果父节点不存在，创建子节点会抛异常
         *  参数2：节点内容，不支持序列化
         *  参数3：节点权限
         *  参数4：节点类型，有持久。临时节点(当前会话有效，使用该特性实现分布式锁)
          */
        //String str1 = zk.create("/testRoot", "testRoot".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //String str2 = zk.create("/testRoot/child", "child".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // 获取子节点，只能获取一层
        List<String> children = zk.getChildren("/testRoot", false);
        for (String path : children){
            System.out.println(path);
            // 获取节点下的数据，必须是节点的全路径
            byte[] data = zk.getData("/testRoot/"+path, false, null);
            System.out.println(new String(data));
        }

        // 判断节点是否存在
        System.out.println(zk.exists("/testRoot",false));

        zk.close();
    }
}
