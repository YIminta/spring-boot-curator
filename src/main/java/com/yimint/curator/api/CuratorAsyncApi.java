package com.yimint.curator.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Desc: Curator 异步 API：
 * 原生API中基本上所有的操作都有提供异步操作，Curator也有提供异步操作的API。
 * 在使用以上针对节点的操作API时，我们会发现每个接口都有一个inBackground()方法可供调用。此接口就是Curator提供的异步调用入口。
 * 对应的异步处理接口为BackgroundCallback。
 * 此接口指提供了一个processResult的方法，用来处理回调结果。
 * 其中processResult的参数event中的getType()包含了各种事件类型，getResultCode()包含了各种响应码。
 */
@Slf4j
public class CuratorAsyncApi {

    public static void main(String[] args) throws Exception {
        String connectionString = "localhost:2181";
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3,Integer.MAX_VALUE);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        curatorFramework.start();

        //ExecutorService executorService = Executors.newFixedThreadPool(10); 生产环境不建议这样创建
        //使用自定义线程池 参数根据实际情况设置 这里仅供参考
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(3, 6, 2, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

        curatorFramework.create()
                .creatingParentsIfNeeded()//递归创建,如果没有父节点,自动创建父节点
                .withMode(CreateMode.PERSISTENT)//节点类型,持久节点
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//设置ACL,和原生API相同
                .inBackground(new BackgroundCallback() {// context参数：传给服务端的参数,会在异步通知中传回来;executor参数：此接口还允许传入一个Executor实例，用一个专门线程池来处理返回结果之后的业务逻辑。
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println("===>响应码：" + event.getResultCode()+",type:" + event.getType());
                        System.out.println("===>Thread of processResult:"+Thread.currentThread().getName());
                        System.out.println("===>context参数回传：" + event.getContext());
                    }
                },"传给服务端的内容,异步会传回来", threadPoolExecutor)
                .forPath("/node10","123456".getBytes());
        Thread.sleep(3000);


        curatorFramework.create()
                .creatingParentsIfNeeded()//递归创建,如果没有父节点,自动创建父节点
                .withMode(CreateMode.PERSISTENT)//节点类型,持久节点
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//设置ACL,和原生API相同
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println("===>响应码：" + event.getResultCode()+",type:" + event.getType());
                        System.out.println("===>Thread of processResult:"+Thread.currentThread().getName());
                        System.out.println("===>context参数回传：" + event.getContext());
                    }
                },"传给服务端的内容,异步会传回来")
                .forPath("/node10","123456".getBytes());
        Thread.sleep(3000);
        threadPoolExecutor.shutdown();
        /**
         * 控制台输出：
         * ===>响应码：0,type:CREATE
         * ===>Thread of processResult:pool-4-thread-1
         * ===>context参数回传：传给服务端的内容,异步会传回来
         * ===>响应码：-110,type:CREATE
         * ===>Thread of processResult:main-EventThread
         * ===>context参数回传：传给服务端的内容,异步会传回来
         *
         * 上面这个程序使用了异步接口inBackground来创建节点，前后两次调用，创建的节点名相同。
         * 从两次返回的event可以看出，第一次返回的响应码是0，表明此次次调用成功，即创建节点成功；
         * 而第二次返回的响应码是-110，表明该节点已经存在，无法重复创建。
         * 这些响应码和ZooKeeper原生的响应码是一致的。
         *
         * 注意:如果自己指定了线程池,那么相应的操作就会在线程池中执行,如果没有指定,那么就会使用Zookeeper的EventThread线程对事件进行串行处理.
         * 在ZooKeeeper中，所有的异步通知事件都是由EventThread这个线程来处理的——EventThread线程用于穿行处理所有的事件通知。
         * EventThread的“串行处理机制”在绝大部分应用场景下能够保证对事件处理的顺序性，
         * 但这个特性也有其弊端，就是一旦碰上一个复杂的处理单元，就会消耗过多的处理时间，从而影响对其他事件得分处理。
         * 因此，在上面的inBacigorund接口中，允许用户传入一个Executor实例，这样一来，就可以把那些复杂的事件处理放到一个专门的线程池中去。
         */
    }
}
