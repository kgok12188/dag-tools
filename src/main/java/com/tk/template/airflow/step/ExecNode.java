package com.tk.template.airflow.step;

import com.tk.template.airflow.AirFlowContext;
import com.tk.template.airflow.MetaInfo;

import java.util.ArrayList;
import java.util.List;

public abstract class ExecNode {

    protected MetaInfo metaInfo;
    protected List<String> nextList = new ArrayList<>(8);
    protected final String name;

    public ExecNode(String name) {
        this.name = name;
    }

    public void setMetaInfo(MetaInfo metaInfo) {
        this.metaInfo = metaInfo;
    }

    public void addNext(String next) {
        nextList.add(next);
    }

    public abstract void run(AirFlowContext airFlowContext);

    public MetaInfo getMetaInfo() {
        return metaInfo;
    }

    public String getName() {
        return name;
    }

    /**
     * 将下游节点，加入执行队列
     * @param flowContext
     */
    protected void addToRunQueue(AirFlowContext flowContext) {
        for (String next : nextList) {
            ExecNode node = metaInfo.getNode(next);
            if (node == null) {
                throw new IllegalArgumentException("not found node : " + next);
            }
            flowContext.addExecNode(node);
        }
    }

}
