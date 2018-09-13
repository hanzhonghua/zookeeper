package com.zookeeper.zkclient;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.List;

/**
 * zkClient：解决原生api的问题：
 *  1：可以级联创建节点
 *  2：在创建、读取、更新操作、判断节点是否存在时候不需要重复的指定watch啦
 */
public class ZkClient1 {

    private static final String CONNECTION_ADDR = "192.168.25.128:2181,192.168.25.133:2181,192.168.25.154:2181";
    private static final int SESSION_TIMEOUT = 3000;

    public static void main(String[] args) {

        ZkClient zkClient = new ZkClient(new ZkConnection(CONNECTION_ADDR), SESSION_TIMEOUT);
        // 创建临时接节点
        // zkClient.createEphemeral("/test1");
        // 相对原生api可以级联创建节点，但是不能设置数据
        //zkClient.createPersistent("/super/test", true);
        // 级联删除
        // zkClient.deleteRecursive("/super");

        /*zkClient.createPersistent("/super","super");
        zkClient.createPersistent("/super/test","test");
        zkClient.createPersistent("/super/test2","test2");
        List<String> list = zkClient.getChildren("/super");
        for (String s : list){
            System.out.println(s);
            String str = "/super/" + s;
            Object obj = zkClient.readData(str);
            System.out.println("节点：" + str + "数据：" + obj);
        }*/

        zkClient.writeData("/super", "super start");
        System.out.println(zkClient.readData("/super"));
        System.out.println(zkClient.exists("/super"));


        try {
            //Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        zkClient.close();
    }
}
