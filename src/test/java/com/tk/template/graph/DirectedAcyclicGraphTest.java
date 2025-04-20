package com.tk.template.graph;

import com.tk.template.graph.exceptions.CircularDependence;
import com.tk.template.tools.MpmcBlockingQueue;
import com.tk.template.tools.SpinPolicy;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

public class DirectedAcyclicGraphTest {


    @Test
    public void testMpmc() throws InterruptedException {
        BlockingQueue<Runnable> blockingQueue = new MpmcBlockingQueue<>(1024 * 1024, SpinPolicy.BLOCKING);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 10,
                0L, TimeUnit.MILLISECONDS,
                blockingQueue);
        for (int i = 0; i < 10; i++) {
            threadPoolExecutor.submit(() -> {
            });
        }

        int n = 1024 * 1024 - 10;
        CountDownLatch countDownLatch = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            blockingQueue.put(countDownLatch::countDown);
        }

        // Thread.sleep(1000);
        countDownLatch.await();
    }

    @Test
    /**
     * 完整的dag 执行
     */
    public void test() throws InterruptedException {
        List<DagWorker<Map<String, String>>> dagWorkers = create();
        DirectedAcyclicGraph<Map<String, String>> directedAcyclicGraph = new DirectedAcyclicGraph<>(dagWorkers);
        int n = 100_000_000;
        CountDownLatch countDownLatch = new CountDownLatch(n);
        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            ConcurrentHashMap<String, String> params = new ConcurrentHashMap<>();
            directedAcyclicGraph.run(new DagCallBack<Map<String, String>>() {
                @Override
                public void onOk(Map<String, String> map) {
                    //  System.out.println("time : " + (System.currentTimeMillis() - startTime));
                    countDownLatch.countDown();
                }

                @Override
                public void onError(Map<String, String> map, Map<String, Throwable> errors) {
                    countDownLatch.countDown();
                }
            }, params);
        }
        countDownLatch.await();
        long l = System.currentTimeMillis() - start;
        System.out.println(1000 / (l / (n * 1.0)));
    }

    @Test
    /**
     * 动态 dag 执行
     */
    public void test01() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<DagWorker<Map<String, String>>> dagWorkers = create();
        DirectedAcyclicGraph<Map<String, String>> directedAcyclicGraph = new DirectedAcyclicGraph<>(dagWorkers);
        long startTime = System.currentTimeMillis();
        HashMap<String, String> params = new HashMap<>();
        directedAcyclicGraph.run(new DagCallBack<Map<String, String>>() {
            @Override
            public void onOk(Map<String, String> map) {
                System.out.println(map);
                System.out.println("time = " + (System.currentTimeMillis() - startTime));
                countDownLatch.countDown();
            }

            @Override
            public void onError(Map<String, String> map, Map<String, Throwable> errors) {
            }
        }, params, "task6");
        countDownLatch.await();
    }


    @Test
    public void testMergeEdge() {
        List<DagWorker<Map<String, String>>> circularDependenceWorker = createMerge();
        DirectedAcyclicGraph<Map<String, String>> directedAcyclicGraph = new DirectedAcyclicGraph<>(circularDependenceWorker);
        System.out.println(directedAcyclicGraph);
    }


    @Test(expected = CircularDependence.class)
    public void testCircularDependence() {
        List<DagWorker<Map<String, String>>> circularDependenceWorker = createCircularDependenceWorker();
        DirectedAcyclicGraph<Map<String, String>> directedAcyclicGraph = new DirectedAcyclicGraph<>(circularDependenceWorker);
        System.out.println(directedAcyclicGraph);
    }

    /*******
     *    正常dag 执行流
     *    task1  ---↓
     *             task4 ----> task7
     *    task2  ---↑           |
     *                          ↓
     *    task3  --> task5 --> task6 --> task8
     *                          |
     *                          ---> task9
     *
     * @return
     */
    private List<DagWorker<Map<String, String>>> create() {
        List<DagWorker<Map<String, String>>> workers = new ArrayList<>();
        workers.add(new TestWorker("task1"));
        workers.add(new TestWorker("task2"));
        workers.add(new TestWorker("task3"));
        workers.add(new TestWorker("task4", "task1", "task2"));
        workers.add(new TestWorker("task5", "task3"));
        workers.add(new TestWorker("task6", "task5", "task7"));
        workers.add(new TestWorker("task7", "task4"));
        workers.add(new TestWorker("task8", "task6"));
        workers.add(new TestWorker("task9", "task6"));
        return workers;
    }


    /**
     * 存在循环依赖  B -> C -> F -> E
     *    A --> B --> C
     *          ↑     ↓
     *    D --> E <-  F
     */
    private List<DagWorker<Map<String, String>>> createCircularDependenceWorker() {
        List<DagWorker<Map<String, String>>> workers = new ArrayList<>();
        workers.add(new DagWorker<Map<String, String>>("A") {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        workers.add(new DagWorker<Map<String, String>>("B", new HashSet<String>() {
            {
                add("A");
                add("E");
            }
        }) {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        workers.add(new DagWorker<Map<String, String>>("C", new HashSet<String>() {
            {
                add("B");
            }
        }) {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        workers.add(new DagWorker<Map<String, String>>("D") {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        workers.add(new DagWorker<Map<String, String>>("E", new HashSet<String>() {
            {
                add("D");
                add("F");
            }
        }) {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        workers.add(new DagWorker<Map<String, String>>("F", new HashSet<String>() {
            {
                add("C");
            }
        }) {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        return workers;
    }


    /**
     *  A -> D 的边可以被优化
     *    F --> A --> B
     *          |     ↓
     *          |     C
     *          |     ↓
     *          --->  D
     * @return
     */
    private List<DagWorker<Map<String, String>>> createMerge() {
        List<DagWorker<Map<String, String>>> workers = new ArrayList<>();
        workers.add(new DagWorker<Map<String, String>>("A", new HashSet<String>() {
            {
                add("F");
            }
        }) {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        workers.add(new DagWorker<Map<String, String>>("B", new HashSet<String>() {
            {
                add("A");
            }
        }) {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        workers.add(new DagWorker<Map<String, String>>("C", new HashSet<String>() {
            {
                add("B");
            }
        }) {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        workers.add(new DagWorker<Map<String, String>>("D", new HashSet<String>() {
            {
                add("A");
                add("C");
            }
        }) {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        workers.add(new DagWorker<Map<String, String>>("F") {
            @Override
            public void run(Map<String, String> params) {

            }
        });
        return workers;
    }


    private static class TestWorker extends DagWorker<Map<String, String>> {

        public TestWorker(String name, String... parents) {
            super(name);
            if (parents != null) {
                for (String parent : parents) {
                    super.getParentNames().add(parent);
                }
            }
        }

        @Override
        public void run(Map<String, String> params) {
//            long start = System.currentTimeMillis();
//            int sleep = 10;
//            try {
//                // System.out.println(this.getName() + "-----" + Thread.currentThread().getName());
//                Thread.sleep(sleep);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            long end = System.currentTimeMillis();
//            params.put(this.getName() + "_start", String.valueOf(start));
//            params.put(this.getName() + "_end", String.valueOf(end));
//            params.put(this.getName() + "_thread", Thread.currentThread().getName());
        }
    }

}
