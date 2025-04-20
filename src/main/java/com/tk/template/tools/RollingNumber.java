package com.tk.template.tools;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class RollingNumber {

    final ContendedAtomicLong tail = new ContendedAtomicLong(1L);
    private long start;
    private final Cell[] buffer;
    private final int mask;
    private final long timeWindowTimeMillis;
    private final int bucketSize;

    private Collection<String> keys;
    private TreeMap<String, Integer> keyIdxMap = new TreeMap<>();
    private TreeMap<Integer, String> idxKeyMap = new TreeMap<>();

    private static final MpmcBlockingQueue<Runnable> reportQueue = new MpmcBlockingQueue<>(2048);
    private static final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static final AtomicBoolean startFlag = new AtomicBoolean(false);

    /**
     * @param timeWindowTimeMillis 时间窗口大小（毫秒）
     * @param bucketSize
     * @param keys
     */
    public RollingNumber(long timeWindowTimeMillis, int bucketSize, Collection<String> keys) {
        this.keys = new HashSet<>(keys);
        this.bucketSize = bucketSize;
        this.timeWindowTimeMillis = timeWindowTimeMillis;
        int c = 2;
        while (c < (bucketSize + 10)) {
            c <<= 1;
        }
        buffer = new Cell[c];
        mask = c - 1;
        start = System.currentTimeMillis() / timeWindowTimeMillis;
        for (int i = 0; i < c; i++) {
            buffer[i] = new Cell(0);
            buffer[i].bucket = new Bucket(this.keys.size() + 1);
        }
        tail.set(start);
        int i = 1;
        for (String key : this.keys) {
            keyIdxMap.put(key, i);
            idxKeyMap.put(i++, key);
        }
        getCurrentBucket();
        startRollingNumber();
    }

    private void startRollingNumber() {
        if (startFlag.compareAndSet(false, true)) {
            executorService.submit(() -> {
                while (true) {
                    try {
                        Runnable runnable = reportQueue.take();
                        runnable.run();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    private Bucket getCurrentBucket() {
        long offset = System.currentTimeMillis() / this.timeWindowTimeMillis;
        long tailOffset;
        Cell cell;
        long diff;
        for (; ; ) {
            cell = buffer[(int) (mask & offset)];
            diff = offset - cell.seq.get();
            if (diff == 0L) {
                return cell.bucket;
            } else if (diff > 0) {
                tailOffset = tail.get();
                if (offset >= tailOffset && tail.compareAndSet(tailOffset, tailOffset + 1)) {
                    cell = buffer[(int) (tailOffset & mask)];
                    cell.seq.set(tailOffset);
                    // 上报上一个时间窗口的数据
                    reportQueue.offer(new Report(buffer[(int) ((tailOffset - 1) & mask)]));
                } else {
                    // 其他线程正在创建 Bucket
                    for (int i = 0; i < 100; i++) {
                        Thread.yield();
                    }
                }
            }
        }
    }

    /**
     * 执行耗时汇报
     * @param key
     * @param time
     */
    public void reportSuccess(String key, long time) {
        int index = keyIdxMap.getOrDefault(key, 0);
        Bucket bucket = getCurrentBucket();
        bucket.successCounter[index].incrementAndGet();
        bucket.longAdder[index].add(time);
        bucket.longMaxCounter[index].update(time);
    }

    /**
     * 错误统计
     * @param key
     */
    public void reportError(String key) {
        int index = keyIdxMap.getOrDefault(key, 0);
        getCurrentBucket().errorCounter[index].incrementAndGet();
    }

    private class Bucket {

        private AtomicInteger[] successCounter;
        private AtomicInteger[] timeoutCounter;
        private AtomicInteger[] errorCounter;
        // 耗时累计，longAdder / successCounter + timeoutCounter = 平均耗时
        private LongAdder[] longAdder;
        // 最大响应时间
        private LongMaxUpdater[] longMaxCounter;
        private int size;

        public Bucket(int size) {
            this.size = size;
            successCounter = new AtomicInteger[size];
            errorCounter = new AtomicInteger[size];
            timeoutCounter = new AtomicInteger[size];
            longAdder = new LongAdder[size];
            longMaxCounter = new LongMaxUpdater[size];
            for (int i = 0; i < size; i++) {
                successCounter[i] = new AtomicInteger();
                timeoutCounter[i] = new AtomicInteger();
                errorCounter[i] = new AtomicInteger();
                longAdder[i] = new LongAdder();
                longMaxCounter[i] = new LongMaxUpdater();
            }
        }

        protected void clear() {
            for (int i = 0; i < size; i++) {
                successCounter[i].set(0);
                errorCounter[i].set(0);
                timeoutCounter[i].set(0);
                longAdder[i].reset();
                longMaxCounter[i].reset();
            }
        }

    }

    private static final class Cell {
        final ContendedAtomicLong seq = new ContendedAtomicLong(0L);
        Bucket bucket;

        Cell(final long s) {
            seq.set(s);
            bucket = null;
        }
    }


    private class Report implements Runnable {
        final Cell cell;
        final Bucket bucket;

        public Report(Cell cell) {
            this.cell = cell;
            this.bucket = cell.bucket;
        }

        @Override
        public void run() {
            buffer[(int) (cell.seq.get() - bucketSize) & mask].bucket.clear();
            report();
        }

        /**
         * 统计信息汇总
         */
        protected void report() {
            try {
                // 清理环形队列数据，复用
                HashMap<String, Object> printValue = new HashMap<>();
                for (int i = 1; i < (keys.size() + 1); i++) {
                    String name = idxKeyMap.get(i);
                    List<? extends Number> numbers = Arrays.asList(bucket.successCounter[i].intValue(), bucket.longAdder[i].sum() / (bucket.successCounter[i].intValue() + 1), bucket.longMaxCounter[i].max());
                    printValue.put(name, numbers);
                }
                Date date = new Date(cell.seq.get() * timeWindowTimeMillis);
                String format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
                System.out.println(format + "" + printValue);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws InterruptedException {
        List<String> list = Arrays.asList("A", "B", "C", "D");

        RollingNumber rollingNumber = new RollingNumber(1000, 16, list);
        Thread.sleep(3_000);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int poolSize = 10;
        CountDownLatch countDownLatch = new CountDownLatch(poolSize);
        for (int k = 0; k < poolSize; k++) {
            executorService.submit(() -> {
                try {
                    int i = 0;
                    do {
                        i++;
                        rollingNumber.reportSuccess(list.get(i & (list.size() - 1)), (long) (Math.random() * 100));
                    } while (i < 100_000_00_00);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        executorService.shutdown();
    }

}
