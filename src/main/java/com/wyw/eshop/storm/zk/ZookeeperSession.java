package com.wyw.eshop.storm.zk;

import com.wyw.eshop.storm.hello.HelloworldTopology;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZookeeperSession {
    private static final Logger logger = Logger.getLogger(HelloworldTopology.class);

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zookeeper;


    public ZookeeperSession() {
        try {
            this.zookeeper = new ZooKeeper("192.168.74.133:2181",
                    50000,
                    new ZookeeperWatcher());
            logger.info(zookeeper.getState());

            connectedSemaphore.await();

            logger.info("Zookeeper session established ....");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void acquireDistributeLock(){
        String path = "/taskid-list-lock";
        try {
            zookeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            logger.info("success to acquire lock for taskid-list-lock ");
        } catch (Exception e) {
            Integer count = 0;
            while (true){
                try {
                    Thread.sleep(1000);
                    zookeeper.create(path,"".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
                } catch (Exception e1) {
                    count++;
                    logger.info("the "+count+" times try to acquire lock for taskid-list-lock.....");
                    continue;
                }
                logger.info("success to acquire lock for " +
                        "taskid-list-lock after + "+count + " times try");
                break;
            }
        }
    }


    public void releaseDistributedLock(){
        String path = "/taskid-list-lock";
        try {
            zookeeper.delete(path,-1);
            logger.info("release the lock for taskid-list-lock");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public String getNodeData(String path){
//        String path = "/taskid-list";
        try {
            byte[] data = zookeeper.getData(path, false, new Stat());
            return new String(data);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void setNodeData(String path,String data){
        try {
            zookeeper.setData(path,data.getBytes(),-1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class ZookeeperWatcher implements Watcher{

        @Override
        public void process(WatchedEvent watchedEvent) {
            if(Event.KeeperState.SyncConnected == watchedEvent.getState()){
                connectedSemaphore.countDown();
            }
        }
    }

    private static class Singleton{
        private static ZookeeperSession instance;
        static {
            instance = new ZookeeperSession();
        }

        public static ZookeeperSession getInstance(){
            return instance;
        }
    }

    public static ZookeeperSession getInstance(){
        return Singleton.getInstance();
    }



    public static void init(){
        getInstance();
    }

}
