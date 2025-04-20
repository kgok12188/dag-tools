package com.tk.template.airflow.step;

import com.tk.template.airflow.AirFlowContext;

public class ExecInclusiveEnd extends ExecNode {

    private String beginNode;

    public ExecInclusiveEnd(String beginNode, String name) {
        super(name);
        this.beginNode = beginNode;
    }

    @Override
    public void run(AirFlowContext airFlowContext) {
        InclusiveCounter inclusiveCounter = airFlowContext.getInclusiveCounter(this.name);
        if (inclusiveCounter.decrement()) {
            addToRunQueue(airFlowContext);
        }
    }

}
