package com.yimint.curator.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Collection;

/**
 * @Desc: Curator 事务 API： curatorFramework.transactionOp()
 * 对于涉及多种基本操作的场景如何保证所有基本操作的原子性呢？答案就是：Curator所提供的事务管理功能。
 */
@Slf4j
public class CuratorTransactionApi {

    //Curator 2.13.0
    public static void test(String[] args) throws Exception {
        String connectionString = "192.168.153.131:2181";
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, Integer.MAX_VALUE);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        curatorFramework.start();

        Collection<CuratorTransactionResult> results = curatorFramework.inTransaction()
                .create().forPath("/a", "some data".getBytes())// create  /a
                .and().create().forPath("/a/path", "other data".getBytes())// create  /a/path
                .and().setData().forPath("/a", "other data".getBytes())//setData  /a
                .and().delete().forPath("/a")// delete  /a
                .and().commit();

        for (CuratorTransactionResult result : results) {
            System.out.println(result.getForPath() + " - " + result.getType());
        }
        /**
         * 上面的例子最后会抛异常KeeperErrorCode = Directory not empty，由于最后一步delete的节点/a存在子节点，所以整个事务commit失败。
         * 注意：事务操作的时候不支持自动创建父节点,也就是说你想创建的节点如果是多层的,那么父节点一定要存在才可以。
         */
    }

    //Curator 5.1.0
    public static void main(String[] args) throws Exception {
        String connectionString = "192.168.153.131:2181";
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3,Integer.MAX_VALUE);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        curatorFramework.start();

        CuratorOp createParentNode = curatorFramework.transactionOp().create().forPath("/a", "some data".getBytes());// create  /a
        CuratorOp createChildNode = curatorFramework.transactionOp().create().forPath("/a/path", "other data".getBytes());// create  /a/path
        CuratorOp setParentNode = curatorFramework.transactionOp().setData().forPath("/a", "other data".getBytes());//setData  /a
        CuratorOp deleteParent = curatorFramework.transactionOp().delete().forPath("/a"); // delete  /a

        Collection<CuratorTransactionResult> results = curatorFramework.transaction().forOperations(createParentNode, createChildNode, setParentNode,deleteParent);

        for ( CuratorTransactionResult result : results ){
            System.out.println(result.getForPath() + " - " + result.getType());
        }
        /**
         * 上面的例子最后会抛异常KeeperErrorCode = Directory not empty，由于最后一步delete的节点/a存在子节点，所以整个事务commit失败。
         * 注意：事务操作的时候不支持自动创建父节点,也就是说你想创建的节点如果是多层的,那么父节点一定要存在才可以。
         */
    }

}
