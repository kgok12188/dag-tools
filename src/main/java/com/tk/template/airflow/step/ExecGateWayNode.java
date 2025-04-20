package com.tk.template.airflow.step;

import com.tk.template.airflow.Condition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class ExecGateWayNode extends ExecNode {

    protected List<Condition> conditions = new ArrayList<>(8);


    public ExecGateWayNode(String name) {
        super(name);
    }

    public void addCondition(Condition condition) {
        conditions.add(condition);
        Collections.sort(conditions);
    }

}
