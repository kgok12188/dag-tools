package com.tk.template.airflow.step;

import com.tk.template.airflow.AirFlowContext;
import com.tk.template.airflow.Condition;

/**
 * 排他网关 开始节点
 */
public class ExecExclusiveBegin extends ExecGateWayNode {


    private String endName;

    public ExecExclusiveBegin(String name, String endName) {
        super(name);
        this.endName = endName;
    }


    public void run(AirFlowContext airFlowContext) {
        System.out.println("ExecExclusiveBegin : " + this.name);
        Condition next = null;
        for (Condition condition : conditions) {
            if (condition.condition(airFlowContext.getDataContext())) {
                next = condition;
                break;
            }
        }
        if (next != null) {
            System.out.println(this.name + "\t:\tnext : " + next.getNext());
            getMetaInfo().getNode(next.getNext()).run(airFlowContext);
        }
    }

}
