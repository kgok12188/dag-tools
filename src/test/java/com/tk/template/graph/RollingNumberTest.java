package com.tk.template.graph;

import com.tk.template.tools.RollingNumber;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RollingNumberTest {

    public static void main(String[] args) throws InterruptedException {
        List<String> list = Arrays.asList("A", "B", "C", "D");

        RollingNumber rollingNumber = new RollingNumber(1000, 16, list);
        Thread.sleep(3_000);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int k = 0; k < 4; k++) {
            executorService.submit(() -> {
                int i = 0;
                do {
                    i++;
                    rollingNumber.reportSuccess(list.get(i & (list.size() - 1)), 1);
//                    try {
//                        Thread.sleep(1);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                } while (i < 100_00_000_00);
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        executorService.shutdown();
    }


}
