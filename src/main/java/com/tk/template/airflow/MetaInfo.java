package com.tk.template.airflow;

import com.tk.template.airflow.step.ExecNode;

import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

/**
 * 1、输入输出数据集 说明
 *     public enum ClazzType {
 *         INPUT, DERIVE, OUTPUT
 *     }
 * 2、execNodeMap
 *      每个节点名称和执行单元的对应关系
 */
public class MetaInfo {

    private Map<String, ClazzType> typeInfo = new TreeMap<>();
    private Map<String, ExecNode> execNodeMap = new TreeMap<>();

    private static final ThreadLocal<LinkedList<MetaInfo>> metaInfoStack = new ThreadLocal<LinkedList<MetaInfo>>() {
        @Override
        protected LinkedList<MetaInfo> initialValue() {
            return new LinkedList<>();
        }
    };

    private MetaInfo() {

    }

    public enum ClazzType {
        INPUT, DERIVE, OUTPUT
    }

    public boolean isInputClazz(Class clz) {
        return clz != null && typeInfo.get(clz.getSimpleName()) == ClazzType.INPUT;
    }

    public boolean isOutputClazz(Class clz) {
        return clz != null && typeInfo.get(clz.getSimpleName()) == ClazzType.OUTPUT;
    }

    public boolean isDeriveClazz(Class clz) {
        return clz != null && typeInfo.get(clz.getSimpleName()) == ClazzType.DERIVE;
    }


    public static class MetaInfoBuilder {

        private MetaInfo metaInfo = new MetaInfo();

        public MetaInfoBuilder addInput(Class clz) {
            metaInfo.typeInfo.put(clz.getSimpleName(), ClazzType.INPUT);
            return this;
        }

        public MetaInfoBuilder addDerive(Class clz) {
            metaInfo.typeInfo.put(clz.getSimpleName(), ClazzType.DERIVE);
            return this;
        }

        public MetaInfoBuilder addOutput(Class clz) {
            metaInfo.typeInfo.put(clz.getSimpleName(), ClazzType.OUTPUT);
            return this;
        }

        public MetaInfo build() {
            return metaInfo;
        }
    }

    public void addNode(String name, ExecNode execNode) {
        execNodeMap.put(name, execNode);
    }

    public ExecNode getNode(String name) {
        return execNodeMap.get(name);
    }

    public static MetaInfoBuilder newBuilder() {
        return new MetaInfoBuilder();
    }

    public static MetaInfo getMetaInfo() {
        return metaInfoStack.get().peek();
    }

    public static void push(MetaInfo metaInfo) {
        metaInfoStack.get().push(metaInfo);
    }

    public static void remove() {
        metaInfoStack.get().poll();
    }
}
