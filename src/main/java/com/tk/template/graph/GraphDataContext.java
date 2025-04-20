package com.tk.template.graph;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据上下文记录请求数据，各任务worker的执行状态
 * @param <T>
 */
class GraphDataContext<T> {

    private final AtomicInteger[] dpCounts;
    // 任务的就绪时间
    private final long[] prepareTimes;
    // 任务的开始时间
    private final long[] startTimes;
    // 任务的结束时间
    private final long[] endTimes;
    // dag 工作节点的包装类，包含下游worker
    public final DagWorkerWrapper<T>[] execDagWorkerWrappers;
    // dag 执行完成后回调函数
    private final DagCallBack<T> callBack;
    private final T params;
    private Map<String, Throwable> errors = new ConcurrentHashMap<>(4);

    public GraphDataContext(DagWorkerWrapper<T>[] execDagWorkerWrappers, DagCallBack<T> callBack, T params) {
        int size = execDagWorkerWrappers.length;
        dpCounts = new AtomicInteger[size];
        prepareTimes = new long[size];
        startTimes = new long[size];
        endTimes = new long[size];
        this.callBack = callBack;
        this.params = params;
        this.execDagWorkerWrappers = execDagWorkerWrappers;
        for (int i = 0; i < size; i++) {
            dpCounts[i] = new AtomicInteger();
        }
    }

    public void setDpCount(int index, int count) {
        dpCounts[index].set(count);
    }

    public void markPrepare(DagWorkerWrapper<T> dagWorkerWrapper) {
        prepareTimes[dagWorkerWrapper.index] = System.currentTimeMillis();
    }

    public void markStart(DagWorkerWrapper<T> dagWorkerWrapper) {
        startTimes[dagWorkerWrapper.index] = System.currentTimeMillis();
    }

    public void markEnd(DagWorkerWrapper<T> dagWorkerWrapper) {
        endTimes[dagWorkerWrapper.index] = System.currentTimeMillis();
    }

    public void runCallBack() {
        if (errors.isEmpty()) {
            callBack.onOk(params);
        } else {
            callBack.onError(params, errors);
        }
    }

    public T getParams() {
        return params;
    }


    public long getPrepareTime(DagWorkerWrapper<T> dagWorkerWrapper) {
        return startTimes[dagWorkerWrapper.index] - prepareTimes[dagWorkerWrapper.index];
    }


    public long getExecTime(DagWorkerWrapper<T> dagWorkerWrapper) {
        return endTimes[dagWorkerWrapper.index] - startTimes[dagWorkerWrapper.index];
    }

    public AtomicInteger getDpCount(DagWorkerWrapper<T> dagWorkerWrapper) {
        return dpCounts[dagWorkerWrapper.index];
    }


    public void putThrowable(String name, Throwable throwable) {
        errors.put(name, throwable);
    }

}
