package com.tk.template.airflow;

import java.util.HashMap;

public class JaninoTest {

    public static void main(String[] args) throws Exception {
        Airflow airflow = AirflowManager.load("a", "p00001", "/Users/scy/Downloads/dag-tools/src/main/resources/p00001");
        DataContext dataContext = new DataContext(
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>()
                , new HashMap<>(),
                new HashMap<>()
        );
        airflow.run(dataContext);
        // System.out.println(execNode);
    }

}
