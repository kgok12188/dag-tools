package com.tk.template.airflow;

import com.tk.template.airflow.step.ExecNode;
import org.codehaus.janino.ClassBodyEvaluator;

import java.io.File;
import java.io.FileReader;

public class AirflowManager {


    public static Airflow load(String project, String version, String path) throws Exception {
        ExecNode start = load(version, path);
        return new Airflow(project + "_" + version, start, start.getMetaInfo());
    }

    private static ExecNode load(String version, String path) throws Exception {
        File dir = new File(path);
        if (dir.isFile()) {
            throw new IllegalArgumentException("只能加载目录");
        } else {
            File[] files = dir.listFiles();
            File main_start = null;
            for (File file : files) {
                if (file.getName().equals("main_start.ruleset")) {
                    main_start = file;
                }
            }
            ExecNode startNode = loadMain_start(main_start);
            MetaInfo metaInfo = startNode.getMetaInfo();
            for (File file : files) {
                if (file.getName().equals("main_start.ruleset")) {
                    continue;
                }
                if (file.isDirectory()) {
                    Airflow subFlow = load(file.getName(), version, file.getAbsolutePath());
                    metaInfo.addNode(file.getName(), subFlow);
                } else {
                    ExecNode execNode = load(file);
                    metaInfo.addNode(execNode.getName(), execNode);
                    execNode.setMetaInfo(startNode.getMetaInfo());
                }
            }
            return startNode;
        }
    }

    private static ExecNode loadMain_start(File file) throws Exception {
        System.out.println("load " + file.getPath());
        ClassBodyEvaluator ce = new ClassBodyEvaluator();
        ce.cook(new FileReader(file));
        return (ExecNode) ce.getClazz().getMethod("getExecNode").invoke(null);
    }

    private static ExecNode load(File file) throws Exception {
        System.out.println("load " + file.getPath());
        ClassBodyEvaluator ce = new ClassBodyEvaluator();
        ce.cook(new FileReader(file));
        return (ExecNode) ce.getClazz().getMethod("getExecNode").invoke(null);
    }

}
