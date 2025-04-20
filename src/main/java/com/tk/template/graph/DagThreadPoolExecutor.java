package com.tk.template.graph;


import com.tk.template.tools.MpmcBlockingQueue;
import com.tk.template.tools.SpinPolicy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class DagThreadPoolExecutor {

    private static final AtomicInteger threadNum = new AtomicInteger(1);
    private int AVAILABLE_PROCESSOR_10_TIMES;
    private BlockingQueue<Runnable> blockingQueue = new MpmcBlockingQueue<>(1024 * 16, SpinPolicy.BLOCKING);// new LinkedBlockingQueue<>();

    private ExecutorService threadPoolExecutor;

    public DagThreadPoolExecutor() {
        this(Runtime.getRuntime().availableProcessors() * 5);
    }

    public DagThreadPoolExecutor(int corePoolSize) {
        this(corePoolSize, "dag-task");
    }

    public DagThreadPoolExecutor(String namePrefix) {
        this(Runtime.getRuntime().availableProcessors() * 5, namePrefix);
    }

    public DagThreadPoolExecutor(int corePoolSize, String namePrefix) {
        AVAILABLE_PROCESSOR_10_TIMES = corePoolSize * 10;
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, corePoolSize, 0L, TimeUnit.MILLISECONDS, blockingQueue,
                r -> new Thread(r, namePrefix + threadNum.getAndIncrement()));
        for (int i = 0; i < corePoolSize; i++) {
            threadPoolExecutor.execute(() -> {
            });
        }
    }

    /**
     * 提交 worker 执行
     * @param dagRunner
     * @param flag ： true   任务队列满阻塞等待
     *                false  排队执行任务过多，加入失败，每个具体执行任务的线程，加入到本地队列
     * @return
     */
    public boolean addToQueue(Runnable dagRunner, boolean flag) {
        if (flag) {
            if (blockingQueue.size() > AVAILABLE_PROCESSOR_10_TIMES) {
                return false;
            }
        } else {
            try {
                blockingQueue.put(dagRunner);
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        }
        return blockingQueue.offer(dagRunner);
    }

}
