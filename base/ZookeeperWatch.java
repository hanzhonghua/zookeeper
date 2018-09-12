package com.zookeeper.base;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ZookeeperWatch implements Watcher {

    private static final int SESSION_TIMEOUT = 10000;
    private static final String CONNECT_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final String PARENT_PATH = "/parent";
    private static final String CHILD_PATH = "/parent/child";
    private static final String LOG_MAIN = "【MAIN】";
    private ZooKeeper zk = null;
    private CountDownLatch cdl = new CountDownLatch(1);
    private AtomicInteger ai = new AtomicInteger(0);

    public void createConnection(String connectionAddr, int sessionTimeout){
        this.releaseConnectionn();
        try {
            zk = new ZooKeeper(connectionAddr, sessionTimeout, this);
            System.out.println(LOG_MAIN + "开始连接zk");
            cdl.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void releaseConnectionn(){
        if(this.zk != null){
            try {
                this.zk.close();
                System.out.println("断开zk连接");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *  创建节点
     * @param path 节点路径
     * @param data 数据
     * @param needWatch 是否需要监控
     * @return
     */
    public Boolean createPath(String path, String data, boolean needWatch){
        try {
            // 监控都是执行一次的，所以每次执行完之后必须重新设置
            this.zk.exists(path, needWatch);
            System.out.println(LOG_MAIN+"节点创建成功，Path:"+
                    // 所有可见(参数3)的永久节点(参数4)
                    this.zk.create(path,data.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT)+
                    "content:"+data);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // 更新节点数据
    public void writeDate(String path, String data) {
        try {
            System.out.println(LOG_MAIN+"更新节点数据成功：path:"+path+" stat:"+this.zk.setData(path,data.getBytes(),-1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 读取节点数据,重试设置Watch，因为Watch只是执行一次
    public String readDate(String path){
        try {
            return new String(this.zk.getData(path,true,null));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public List<String> readChild(String path, Boolean watch){
        try {
            List<String> children = this.zk.getChildren(path, watch);
            return children;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void process(WatchedEvent watchedEvent) {

        // 事件类型(与连接数据相关)
        Event.EventType type = watchedEvent.getType();
        // 连接状态(与连接zk服务状态相关)
        Event.KeeperState state = watchedEvent.getState();
        // 受影响的path
        String path = watchedEvent.getPath();
        String logWatcher = "【Watcher-"+ai.incrementAndGet()+"】";

        System.out.println(logWatcher + "收到Watcher通知");
        System.out.println(logWatcher + "连接状态：\t"+state);
        System.out.println(logWatcher + "事件类型：\t"+type);

        // 首次连接
        if(Event.KeeperState.SyncConnected == state){
            if(Event.EventType.None == type){
                System.out.println(logWatcher+"与ZK成功建立连接");
                cdl.countDown();
            }else if(Event.EventType.NodeCreated == type){ // 创建节点
                System.out.println(logWatcher+"创建节点");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else if (Event.EventType.NodeDataChanged == type){
                System.out.println(logWatcher+"节点数据更新");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else if (Event.EventType.NodeChildrenChanged == type){
                System.out.println(logWatcher+"子节点变更");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else if (Event.EventType.NodeDeleted == type){
                System.out.println(logWatcher+"子节点被删除：" +path);
            }
        }else if(Event.KeeperState.Disconnected == state){
            System.out.println(logWatcher+"与ZK断开连接");
        }else if(Event.KeeperState.AuthFailed == state){
            System.out.println(logWatcher+"权限检查失败");
        }else if(Event.KeeperState.Expired == state){
            System.out.println(logWatcher+"会话失效");
        }
        System.out.println("-----------------------------------------");
    }

    public static void main(String[] args) throws Exception {
        ZookeeperWatch zkw = new ZookeeperWatch();
        zkw.createConnection(CONNECT_ADDR, SESSION_TIMEOUT);

        // 创建节点
        if(zkw.createPath(PARENT_PATH,"Hello World",true)){
            Thread.sleep(1000);
        }

        // 读取数据
        System.out.println("Path："+PARENT_PATH+"数据："+zkw.readDate(PARENT_PATH));

        System.out.println("Path："+PARENT_PATH+"子节点："+zkw.readChild(PARENT_PATH, true));

        // 更新节点数据
        zkw.writeDate(PARENT_PATH,"哈哈哈");

        zkw.createPath("CHILD_PATH","",true);

        Thread.sleep(10000);
        zkw.releaseConnectionn();
    }
}
