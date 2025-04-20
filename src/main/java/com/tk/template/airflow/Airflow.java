package com.tk.template.airflow;

import com.tk.template.airflow.step.ExecNode;


public class Airflow extends ExecNode {

    private final ExecNode start;
    private final MetaInfo metaInfo;

    public Airflow(String name, ExecNode start, MetaInfo metaInfo) {
        super(name);
        this.start = start;
        this.metaInfo = metaInfo;
    }

    public void run(DataContext dataContext) {
        AirFlowContext airFlowContext = new AirFlowContext(dataContext);
        System.out.println("开始运行流程：" + this.getName());
        MetaInfo.push(metaInfo);
        run(airFlowContext);
        MetaInfo.remove();
        System.out.println("结束运行流程：" + this.getName());
    }

    @Override
    public void run(AirFlowContext airFlowContext) {
        airFlowContext.addExecNode(start);
        do {
            ExecNode execNode = airFlowContext.poll();
            if (execNode instanceof Airflow) {
                Airflow subFlow = (Airflow) execNode;
                subFlow.run(airFlowContext.getDataContext());
            } else {
                execNode.run(airFlowContext);
            }
        } while (airFlowContext.notEnd());
    }

}
