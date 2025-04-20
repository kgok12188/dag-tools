package com.tk.template.airflow.step;

import com.tk.template.airflow.AirFlowContext;
import com.tk.template.airflow.DataContext;

public abstract class ExecSimpleNode extends ExecNode {

    public ExecSimpleNode(String name) {
        super(name);
    }

    public void run(AirFlowContext airFlowContext) {
        exec(airFlowContext.getDataContext());
        addToRunQueue(airFlowContext);
    }

    public abstract void exec(DataContext dataContext);


}
