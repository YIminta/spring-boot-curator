package com.yimint.curator;

import com.yimint.curator.client.CuratorClient;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;

@SpringBootTest
class CuratorApplicationTests {

    @Resource
    private CuratorClient curatorClient;

    @Test
    public void testCuratorClient() {
        curatorClient.createNode(CreateMode.EPHEMERAL, "/test/abc", "123");
        String data = curatorClient.getNodeData("/test/abc");
        System.out.println(data);
    }

    @Test
    public void testMutexLock() {
        CountDownLatch latch = new CountDownLatch(2);
        Thread t1 = new Thread(() -> {
            InterProcessLock lock = curatorClient.getMutexLock("/lock");
            curatorClient.acquire(lock);
            System.out.println("t1获得了锁");
            System.out.println("等待1秒。。。");
            sleep(1000);
            curatorClient.release(lock);
            System.out.println("t1释放了锁");
            latch.countDown();
        });
        Thread t2 = new Thread(() -> {
            InterProcessLock lock = curatorClient.getMutexLock("/lock");
            curatorClient.acquire(lock);
            System.out.println("t2获得了锁");
            System.out.println("等待2秒。。。");
            sleep(2000);
            curatorClient.release(lock);
            System.out.println("t2释放了锁");
            latch.countDown();
        });
        t1.start();
        t2.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
