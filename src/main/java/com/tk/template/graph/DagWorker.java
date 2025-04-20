package com.tk.template.graph;

import java.util.HashSet;
import java.util.Set;

public abstract class DagWorker<T> {


    /**
     * 耗时操作，比如发起http请求
     */
    public static final int IO = 1;
    /**
     * 纯cpu操作，耗时短的任务节点
     */
    public static final int CPU = 0;

    /**
     * 依赖的父节点步骤名称
     */
    protected Set<String> parentNames;
    /**
     * 当前步骤名称
     */
    protected String name;


    public DagWorker(String name) {
        this(name, new HashSet<>());
    }

    public DagWorker(String name, Set<String> parentNames) {
        this.name = name;
        if (parentNames == null) {
            this.parentNames = new HashSet<>(2);
        } else {
            this.parentNames = parentNames;
        }
    }

    public Set<String> getParentNames() {
        return parentNames;
    }

    public String getName() {
        return name;
    }

    public int runType() {
        return IO;
    }

    public abstract void run(T params);


}
