package com.tk.template.airflow;

public class ClazzInfoTest {


    public static void main(String[] args) {
        MetaInfo metaInfo = MetaInfo.newBuilder().addDerive(String.class).addInput(Integer.class).addOutput(Double.class).build();
        System.out.println(metaInfo.isInputClazz(Integer.class));
        System.out.println(metaInfo.isInputClazz(Double.class));
    }

}
