package com.zookeeper.acl;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 在操作zk节点的时候，可以加上一些权限验证
 * 主要有三种模式
 * 1.指定ip
 * 2.Disgest：最常用的模式，类似用户名密码的权限标示，zk会对权限标示两次加密SHA-1、BASE64
 * 3.Super，超级管理员模式，该模式下可以对zk节点任意操作
 * 创建连接时候指定授权,<只是针对授权创建的节点，其他节点不影响>
 */
public class ZookeeperAuth implements Watcher {

    private static final String CONNECT_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final String PATH = "/testPath";
    private static final String PATH_DEL = "/testPath/delNode";
    private static final String AUTH_TYPE="digest";
    // 认证正确方法
    private static final String AUTH = "123456";
    // 认证错误方法
    private static final String ERROR_AUTH = "654321";
    static ZooKeeper zk = null;
    AtomicInteger seq = new AtomicInteger();
    private static final String LOGGER = "【Main】";
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent == null){
            return;
        }
        // 连接状态
        Event.KeeperState state = watchedEvent.getState();
        // 事件类型
        Event.EventType type = watchedEvent.getType();
        // 受影响的路径
        String path = watchedEvent.getPath();
        String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";
        System.out.println(logPrefix + "收到Watcher通知");
        System.out.println(logPrefix + "连接状态：" + state);
        System.out.println(logPrefix + "连接事件类型：" +type);

        if(Event.KeeperState.SyncConnected == state){
            if(Event.EventType.None == type){
                System.out.println(logPrefix + "成功连上zk服务器");
                connectedSemaphore.countDown();
            }
        } else if (Event.KeeperState.Disconnected == state){
            System.out.println(logPrefix + "与zk断开连接");
        }else if (Event.KeeperState.AuthFailed == state){
            System.out.println(logPrefix + "权限检查失败");
        }else if (Event.KeeperState.Expired == state){
            System.out.println(logPrefix + "会话失效");
        }
    }

    /**
     * 创建zk连接
     * @param connectAddr
     * @param sessionTimeout
     */
    public void createConnection(String connectAddr, Integer sessionTimeout){
        this.closeConnection();
        try {
            zk = new ZooKeeper(connectAddr, sessionTimeout, this);
            // 添加节点授权
            zk.addAuthInfo(AUTH_TYPE, AUTH.getBytes());
            System.out.println(LOGGER + "开始连接zk服务器");
            connectedSemaphore.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // 关闭连接
    public void closeConnection(){
        if(zk != null){
            try {
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        ZookeeperAuth zookeeperAuth = new ZookeeperAuth();
        zookeeperAuth.createConnection(CONNECT_ADDR, 2000);
        List<ACL> acls = new ArrayList<ACL>(1);
        for (ACL acl : ZooDefs.Ids.CREATOR_ALL_ACL){
            acls.add(acl);
        }

        try {
            zk.create(PATH, "init_content".getBytes(), acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key：" + AUTH + "创建节点：" +PATH + "初始化内容：init_content");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            zk.create(PATH_DEL, "will be delleted".getBytes(), acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key：" + AUTH + "创建节点：" +PATH_DEL + "初始化内容：will be delleted");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 不使用授权读取数据，是读取不了数据的
        //getDataNoAuth();
        // 正确授权信息
        //getDataWithAuth();
        // 错误授权信息
        getDataWithErrorAuth();
    }

    // 不使用授权访问数据
    private static void getDataNoAuth() {
        String prefix = "不使用任何授权信息";
        System.out.println(prefix + "获取数据：" +PATH);
        try {
            ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            Thread.sleep(1000);
            System.out.println(prefix + "成功获取数据：" + zk.getData(PATH,false,null));
        } catch (Exception e) {
            System.out.println(prefix + "获取数据失败，原因：" + e.getMessage());
        }
    }

    // 使用正确授权访问数据
    private static void getDataWithAuth() {
        String prefix = "使用正确授权信息";
        System.out.println(prefix + "获取数据：" +PATH);
        try {
            Thread.sleep(1000);
            System.out.println(prefix + "成功获取数据：" + new String(zk.getData(PATH,false,null)));
        } catch (Exception e) {
            System.out.println(prefix + "获取数据失败，原因：" + e.getMessage());
        }
    }

    // 不使用授权访问数据
    private static void getDataWithErrorAuth() {
        String prefix = "使用错误授权信息";
        System.out.println(prefix + "获取数据：" +PATH);
        try {
            ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            zk.addAuthInfo(AUTH_TYPE,ERROR_AUTH.getBytes());
            Thread.sleep(1000);
            System.out.println(prefix + "成功获取数据：" + zk.getData(PATH,false,null));
        } catch (Exception e) {
            System.out.println(prefix + "获取数据失败，原因：" + e.getMessage());
        }
    }
}
