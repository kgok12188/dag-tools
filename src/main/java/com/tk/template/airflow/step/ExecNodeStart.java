package com.tk.template.airflow.step;

import com.tk.template.airflow.AirFlowContext;

public class ExecNodeStart extends ExecNode {

    public ExecNodeStart(String name) {
        super(name);
    }

    @Override
    public void run(AirFlowContext airFlowContext) {
        addToRunQueue(airFlowContext);
    }
}
