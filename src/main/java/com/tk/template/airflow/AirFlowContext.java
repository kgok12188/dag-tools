package com.tk.template.airflow;

import com.tk.template.airflow.step.ExecNode;
import com.tk.template.airflow.step.InclusiveCounter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class AirFlowContext {

    private final LinkedList<ExecNode> queue = new LinkedList<>();
    private final DataContext dataContext;
    private Map<String, InclusiveCounter> counterMap = new HashMap<>();

    public AirFlowContext(DataContext dataContext) {
        this.dataContext = dataContext;
    }

    public void addExecNode(ExecNode node) {
        queue.add(node);
    }

    public ExecNode poll() {
        return queue.poll();
    }

    public boolean notEnd() {
        return queue.size() > 0;
    }

    public DataContext getDataContext() {
        return dataContext;
    }

    public void setCounter(String name, InclusiveCounter inclusiveCounter) {
        InclusiveCounter counter = counterMap.put(name, inclusiveCounter);
        if (counter != null) {
            throw new IllegalArgumentException(name);
        }
    }

    public InclusiveCounter getInclusiveCounter(String name) {
        return counterMap.get(name);
    }

}
