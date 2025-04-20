package com.tk.template.airflow.step;

import com.tk.template.airflow.AirFlowContext;
import com.tk.template.airflow.Condition;

import java.util.ArrayList;
import java.util.List;

public class ExecInclusiveBegin extends ExecGateWayNode {

    private String endNode;

    public ExecInclusiveBegin(String name, String endNode) {
        super(name);
        this.endNode = endNode;
    }

    @Override
    public void run(AirFlowContext airFlowContext) {
        System.out.println("ExecInclusiveBegin : " + this.name);
        List<Condition> nextConditions = new ArrayList<>();
        for (Condition condition : conditions) {
            if (condition.condition(airFlowContext.getDataContext())) {
                nextConditions.add(condition);
            }
        }
        if (nextConditions.size() > 0) {
            airFlowContext.setCounter(endNode, new InclusiveCounter(nextConditions.size()));
        }
        for (Condition next : nextConditions) {
            airFlowContext.addExecNode(metaInfo.getNode(next.getNext()));
        }
    }

}
