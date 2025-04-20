package com.tk.template.airflow.step;

import com.tk.template.airflow.AirFlowContext;

/**
 * 排他网关 结束节点
 */
public class ExecExclusiveEnd extends ExecNode {

    private String beginNode;

    public ExecExclusiveEnd(String beginNode, String name) {
        super(name);
        this.beginNode = beginNode;
    }

    @Override
    public void run(AirFlowContext airFlowContext) {
        System.out.println("ExecExclusiveEnd : " + this.name);
        addToRunQueue(airFlowContext);
    }

}
