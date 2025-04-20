package com.tk.template.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

class DagWorkerWrapper<T> extends DagWorker<T> {

    protected DagWorker<T> dagWorker;
    protected int index;
    protected Map<String, DagWorkerWrapper<T>> nextWorkers = new HashMap<>(8);
    private boolean isEnd = false;

    public DagWorkerWrapper(String name) {
        super(name);
        super.parentNames = new HashSet<>(4);
    }

    public DagWorkerWrapper(DagWorker<T> dagWorker) {
        super(dagWorker.getName(), dagWorker.parentNames);
        this.dagWorker = dagWorker;
    }

    public boolean isEnd() {
        return isEnd || dagWorker == null;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }

    @Override
    public void run(T params) {
        dagWorker.run(params);
    }

    @Override
    public int runType() {
        return dagWorker == null ? CPU : dagWorker.runType();
    }

}
