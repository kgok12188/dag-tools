package com.tk.template.graph;

import com.tk.template.graph.exceptions.CircularDependence;
import com.tk.template.graph.exceptions.NotFoundNode;
import com.tk.template.tools.RollingNumber;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DirectedAcyclicGraph<T> {

    private final DagThreadPoolExecutor dagThreadPoolExecutor;

    public static final String CALL_BACK_NODE_NAME = "##_callback_##";

    private final LocalWorkers<DagRunner> localWorkers = new LocalWorkers<>();

    private final Map<String, DagWorker<T>> originalWorkers;

    //静态dag的执行节点
    private final DagWorkerWrapper<T>[] execDagWorkerWrappers;

    private final RollingNumber rollingNumber;

    public DirectedAcyclicGraph(List<DagWorker<T>> workers) {
        this(workers, null);
    }

    public DirectedAcyclicGraph(List<DagWorker<T>> workers, String namePrefix) {
        this(workers, namePrefix, Runtime.getRuntime().availableProcessors() * 5);
    }


    /**
     * @param workers    任务单例对象
     * @param namePrefix 线程名称前缀
     * @param poolSize   线程个数
     */
    public DirectedAcyclicGraph(List<DagWorker<T>> workers, String namePrefix, int poolSize) {
        HashMap<String, DagWorker<T>> workersMap = new HashMap<>();
        Iterator<DagWorker<T>> iterator = workers.iterator();
        while (iterator.hasNext()) {
            DagWorker<T> dagWorker = iterator.next();
            DagWorker<T> before = workersMap.put(dagWorker.getName(), dagWorker);
            if (before != null && dagWorker != before) {
                throw new IllegalArgumentException(dagWorker.getName() + " 重名!");
            }
        }
        this.originalWorkers = Collections.unmodifiableMap(workersMap);
        execDagWorkerWrappers = getExecGraph(originalWorkers.keySet(), originalWorkers, true);
        this.dagThreadPoolExecutor = new DagThreadPoolExecutor(poolSize, namePrefix);
        this.rollingNumber = new RollingNumber(1000, 16, workersMap.keySet());
    }

    /**
     * 1、检查是否存在循环依赖
     * 2、构建 dag 图
     *
     * @param nodes   需要执行的节点
     * @param workers 所有节点
     * @param merge   是否优化边
     */
    private DagWorkerWrapper<T>[] getExecGraph(Collection<String> nodes, Map<String, DagWorker<T>> workers, boolean merge) {
        /**
         * 获得所有需要执行的节点
         */
        Map<String, DagWorker<T>> execNodes = getExecNodes(nodes, workers);
        Map<String, DagWorkerWrapper<T>> wrapperMap = new HashMap<>();
        Iterator<String> iterator = execNodes.keySet().iterator();
        // 拓扑排序 初始条件是没有依赖的节点
        LinkedList<String> topologicalSortQueue = new LinkedList<>();
        // 构建没有上下游的关系的执行节点
        while (iterator.hasNext()) {
            String key = iterator.next();
            DagWorker<T> dagWorker = execNodes.get(key);
            wrapperMap.put(key, new DagWorkerWrapper<>(dagWorker));
            if (dagWorker.getParentNames().isEmpty()) {
                topologicalSortQueue.add(key);
            }
        }
        iterator = wrapperMap.keySet().iterator();

        // 构建通知关系
        while (iterator.hasNext()) {
            String current = iterator.next();
            DagWorkerWrapper<T> currentDagWorkerWrapper = wrapperMap.get(current);
            Set<String> parentNames = currentDagWorkerWrapper.getParentNames();
            for (String pName : parentNames) {
                DagWorkerWrapper<T> parentDagWorkerWrapper = wrapperMap.get(pName);
                parentDagWorkerWrapper.nextWorkers.put(current, currentDagWorkerWrapper);
            }
        }
        int index = 0;
        DagWorkerWrapper<T>[] execDagWorkerWrappers = new DagWorkerWrapper[execNodes.size() + 1];
        HashSet<String> isVisited = new HashSet<>();
        String node = null;
        // 候选优化边
        LinkedList<MergeNode> candidateMergeNodes = new LinkedList<>();
        while (!topologicalSortQueue.isEmpty()) {
            String current = topologicalSortQueue.pop();
            isVisited.add(current);
            DagWorkerWrapper<T> dagWorkerWrapper = wrapperMap.get(current);
            execDagWorkerWrappers[index] = dagWorkerWrapper;
            dagWorkerWrapper.index = index;
            Map<String, DagWorkerWrapper<T>> nextWorkers = dagWorkerWrapper.nextWorkers;
            for (String next : nextWorkers.keySet()) {
                Set<String> parentNames = execNodes.get(next).getParentNames();
                boolean pNodeVisited = true;
                for (String pName : parentNames) {
                    if (!isVisited.contains(pName)) {
                        pNodeVisited = false;
                        break;
                    }
                }
                if (pNodeVisited) {
                    topologicalSortQueue.add(next);
                } else {
                    if (merge) {
                        candidateMergeNodes.add(new MergeNode(next, current));
                    }
                    node = next;
                }
            }
            index++;
        }
        // 循环依赖
        if (index != execNodes.size()) {
            LinkedList<String> circularDependenceNodes = new LinkedList<>();
            circularDependenceNodes.add(node);
            searchCircularDependenceNodes(workers, circularDependenceNodes);
        }
        // 图的优化，去掉无效边
        if (merge && !candidateMergeNodes.isEmpty()) {
            mergeEdge(candidateMergeNodes, execDagWorkerWrappers);
        }

        // 最后一个节点，关联回调函数
        DagWorkerWrapper<T> callBackNode = new DagWorkerWrapper<>(CALL_BACK_NODE_NAME);
        execDagWorkerWrappers[execDagWorkerWrappers.length - 1] = callBackNode;
        callBackNode.index = execDagWorkerWrappers.length - 1;
        callBackNode.setEnd(true);
        for (int i = 0; i < execDagWorkerWrappers.length - 1; i++) {
            if (execDagWorkerWrappers[i].nextWorkers.isEmpty()) {
                execDagWorkerWrappers[i].nextWorkers.put(callBackNode.name, callBackNode);
                callBackNode.getParentNames().add(execDagWorkerWrappers[i].getName());
            }
        }
        return execDagWorkerWrappers;
    }


    /**
     * 1、去掉无效边
     *
     * @param candidateMergeNodes   可能是能够被优化的边
     * @param execDagWorkerWrappers 所有执行节点
     */
    private void mergeEdge(LinkedList<MergeNode> candidateMergeNodes, DagWorkerWrapper<T>[] execDagWorkerWrappers) {
        // 能够被优化的边
        LinkedList<MergeNode> mergeNodes = new LinkedList<>();
        // MergeNode node 从其他路径，也能找到 pNode , 那么该边可以被优化
        for (MergeNode mergeNode : candidateMergeNodes) {
            LinkedList<String> queue = new LinkedList<>();
            Set<String> parentNodes = originalWorkers.get(mergeNode.node).getParentNames();
            // 去掉pNode 作为初始数据
            for (String pNode : parentNodes) {
                if (!pNode.equals(mergeNode.pNode)) {
                    queue.add(pNode);
                }
            }
            while (!queue.isEmpty()) {
                String cNode = queue.pop();
                if (cNode.equals(mergeNode.pNode)) {
                    // 可以优化的边
                    mergeNodes.add(mergeNode);
                    System.out.println("优化边：" + mergeNode.node + ",\t" + mergeNode.pNode);
                    break;
                } else {
                    parentNodes = originalWorkers.get(cNode).getParentNames();
                    queue.addAll(parentNodes);
                }
            }
        }
        // 去掉关联关系
        for (MergeNode mergeNode : mergeNodes) {
            originalWorkers.get(mergeNode.node).getParentNames().remove(mergeNode.pNode);
            for (int i = 0; i < execDagWorkerWrappers.length - 1; i++) {
                if (mergeNode.node.equals(execDagWorkerWrappers[i].getName())) {
                    execDagWorkerWrappers[i].getParentNames().remove(mergeNode.pNode);
                }
                if (mergeNode.pNode.equals(execDagWorkerWrappers[i].getName())) {
                    execDagWorkerWrappers[i].nextWorkers.remove(mergeNode.node);
                }
            }
        }
    }

    /**
     * 找出构成形成一个环的所有的节点 （ 一个图可能有多个环，找到其中一个环为止）
     *
     * @param workers                 节点
     * @param circularDependenceNodes 构成环的节点
     */
    private void searchCircularDependenceNodes(Map<String, DagWorker<T>> workers, LinkedList<String> circularDependenceNodes) {
        String first = circularDependenceNodes.getFirst();
        String last = circularDependenceNodes.getLast();
        Set<String> parentNodes = workers.get(last).getParentNames();
        for (String pNode : parentNodes) {
            if (pNode.equals(first)) {
                throw new CircularDependence(circularDependenceNodes);
            }
            circularDependenceNodes.add(pNode);
            searchCircularDependenceNodes(workers, circularDependenceNodes);
            circularDependenceNodes.removeLast();
        }
    }

    /**
     * 运行时，关联回调函数，创建执行中间转态对象
     *
     * @param execDagWorkerWrappers 当前dag的所有节点
     * @param params                运行参数
     */
    private GraphDataContext<T> createInFlyAttachCallBack(DagWorkerWrapper<T>[] execDagWorkerWrappers, DagCallBack<T> callBack, T params) {
        GraphDataContext<T> graphDataContext = new GraphDataContext<>(execDagWorkerWrappers, callBack, params);
        for (int i = 0; i < execDagWorkerWrappers.length; i++) {
            graphDataContext.setDpCount(i, execDagWorkerWrappers[i].getParentNames().size());
        }
        return graphDataContext;
    }

    /**
     * 动态计算需要执行的节点
     * 完整的有向无环图图找子图
     *
     * @param nodes   需要执行的节点
     * @param workers 所有的执行节点
     * @return
     */
    private Map<String, DagWorker<T>> getExecNodes(Collection<String> nodes, Map<String, DagWorker<T>> workers) {
        LinkedList<String> execQueue = new LinkedList<>();
        HashSet<String> addedNode = new HashSet<>(nodes.size());
        for (String node : nodes) {
            addedNode.add(node);
            execQueue.add(node);
        }
        HashMap<String, DagWorker<T>> execMap = new HashMap<>();
        while (!execQueue.isEmpty()) {
            String currentNode = execQueue.pop();
            DagWorker<T> tDagWorker = workers.get(currentNode);
            if (tDagWorker == null) {
                Set<Map.Entry<String, DagWorker<T>>> entries = workers.entrySet();
                StringBuilder stringBuffer = new StringBuilder();
                for (Map.Entry<String, DagWorker<T>> item : entries) {
                    if (item.getValue().getParentNames().contains(currentNode)) {
                        stringBuffer.append(",").append(item.getValue().getName());
                    }
                }
                String beforeNodes = stringBuffer.toString();
                throw new NotFoundNode(String.format("节点 %s 的父节点 %s 不存在!", beforeNodes.substring(1), currentNode));
            }
            execMap.put(currentNode, tDagWorker);
            Set<String> parentNodes = tDagWorker.getParentNames();
            for (String pNode : parentNodes) {
                if (!addedNode.contains(pNode)) {
                    execQueue.add(pNode);
                }
            }
        }
        return execMap;
    }

    /**
     * 执行dag
     *
     * @param callBack ： 回调函数
     * @param params   ： 参数
     * @param nodes    : 需要执行的任务名称
     */
    public void run(DagCallBack<T> callBack, T params, Collection<String> nodes) {
        DagWorkerWrapper<T>[] execList;
        if (nodes == null || nodes.isEmpty()) {
            execList = execDagWorkerWrappers;
        } else {
            // 根据 nodes 获得执行子图
            execList = getExecGraph(nodes, originalWorkers, false);
        }
        GraphDataContext<T> graphDataContext = createInFlyAttachCallBack(execList, callBack, params);
        LinkedList<DagRunner> batchList = new LinkedList<>();
        for (DagWorkerWrapper<T> worker : execList) {
            if (worker.getParentNames().isEmpty()) {
                graphDataContext.markPrepare(worker);
                DagRunner dagRunner = new DagRunner(worker, graphDataContext);
                if (worker.runType() == DagWorker.CPU) {
                    batchList.add(dagRunner);
                } else {
                    dagThreadPoolExecutor.addToQueue(dagRunner, false);
                }
            } else {
                break;
            }
        }
        if (!batchList.isEmpty()) {
            dagThreadPoolExecutor.addToQueue(new BatchDagRunner(batchList), false);
        }
    }

    /**
     * 执行dag
     *
     * @param callBack 回调函数
     * @param params   参数
     * @param nodes    运行的节点
     */
    public void run(DagCallBack<T> callBack, T params, String... nodes) {
        run(callBack, params, Arrays.asList(nodes));
    }

    protected class DagRunner implements Runnable {

        protected final DagWorkerWrapper<T> workerWrapper;
        protected final GraphDataContext<T> graphDataContext;

        public DagRunner(DagWorkerWrapper<T> workerWrapper, GraphDataContext<T> graphDataContext) {
            this.workerWrapper = workerWrapper;
            this.graphDataContext = graphDataContext;
        }

        @Override
        public void run() {
            Queue<DagRunner> dagRunnerQueue = localWorkers.get();
            dagRunnerQueue.add(this);
            while (!dagRunnerQueue.isEmpty() && !Thread.currentThread().isInterrupted()) {
                dagRunnerQueue.poll().run0();
            }
            // dagRunnerQueue 还有任务，说明线程被中断，剩余任务提交到全局任务队列
            while (!dagRunnerQueue.isEmpty()) {
                dagThreadPoolExecutor.addToQueue(dagRunnerQueue.poll(), false);
            }
        }

        protected void run0() {
            graphDataContext.markStart(workerWrapper);
            Throwable e = null;
            try {
                if (workerWrapper.isEnd()) {
                    graphDataContext.runCallBack();
                } else {
                    workerWrapper.run(graphDataContext.getParams());
                }
            } catch (Throwable throwable) {
                graphDataContext.putThrowable(workerWrapper.getName(), throwable);
                e = throwable;
            }
            graphDataContext.markEnd(workerWrapper);
            if (e != null) {
                rollingNumber.reportError(workerWrapper.getName());
            } else {
                rollingNumber.reportSuccess(workerWrapper.getName(), graphDataContext.getExecTime(workerWrapper));
            }
            checkRunNext();
        }

        /**
         * 通知下游节点执行
         */
        private void checkRunNext() {
            // 本地排队队列
            Queue<DagRunner> localQueue = localWorkers.get();
            // 被唤醒的IO任务
            for (Map.Entry<String, DagWorkerWrapper<T>> nextWorkerKv : workerWrapper.nextWorkers.entrySet()) {
                DagWorkerWrapper<T> nextWorker = nextWorkerKv.getValue();
                AtomicInteger dpCount = graphDataContext.getDpCount(nextWorker);
                // 检查所依赖的所有任务是否执行完成
                if (dpCount.decrementAndGet() == 0) {
                    graphDataContext.markPrepare(nextWorker);
                    DagRunner nextDagRunner = new DagRunner(nextWorker, graphDataContext);
                    if (nextWorker.runType() == DagWorker.CPU || localQueue.isEmpty()) {
                        graphDataContext.markPrepare(nextWorker);
                        localQueue.add(nextDagRunner);
                        // 尝试加入全局队列，加入失败，加入到本地队列
                    } else if (!dagThreadPoolExecutor.addToQueue(nextDagRunner, true)) {
                        localQueue.add(nextDagRunner);
                    }
                }
            }
        }
    }

    private class BatchDagRunner extends DagRunner {

        private final Queue<DagRunner> runnerList;

        public BatchDagRunner(Queue<DagRunner> runnerList) {
            super(null, null);
            this.runnerList = runnerList;
        }

        @Override
        protected void run0() {
            localWorkers.get().addAll(runnerList);
        }
    }

    private static class MergeNode {
        public final String node;
        public final String pNode;

        public MergeNode(String node, String pNode) {
            this.node = node;
            this.pNode = pNode;
        }
    }

}
