package com.tk.template.airflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author rongyingjie
 */
public class DataContext {

    private static final String DERIVE = "derive";

    private final Map<String, List<Map<String, Object>>> input;
    private final Map<String, List<Map<String, Object>>> drive;
    private final Map<String, List<Map<String, Object>>> output;
    private final Map<String, Object> inputDefaultValue;
    private final Map<String, Object> driveDefaultValue;
    private final Map<String, Object> outputDefaultValue;
    private MetaInfo metaInfo;

    public DataContext(Map<String, Object> inputDefaultValue,
                       Map<String, Object> driveDefaultValue,
                       Map<String, Object> outputDefaultValue,
                       Map<String, List<Map<String, Object>>> input,
                       Map<String, List<Map<String, Object>>> drive,
                       Map<String, List<Map<String, Object>>> output) {
        this.inputDefaultValue = inputDefaultValue;
        this.driveDefaultValue = driveDefaultValue;
        this.outputDefaultValue = outputDefaultValue;
        this.input = input;
        this.drive = drive;
        this.output = output;
    }

    public void setMetaInfo(MetaInfo metaInfo) {
        this.metaInfo = metaInfo;
    }

    public String getString(Class clz, String field) {
        Object value = getValue(clz, field);
        if (value == null) {
            return "";
        } else {
            return String.valueOf(value);
        }
    }

    public double getDouble(Class clz, String field) {
        Object value = getValue(clz, field);
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble(value.toString().trim());
            } catch (Exception e) {
                return 0.0;
            }
        }
        return 0.0;
    }

    private Object getValue(Class clz, String field) {
        List<Map<String, Object>> maps;
        Map<String, Object> defaultValue;
        if (metaInfo.isInputClazz(clz)) {
            maps = input.get(clz.getSimpleName());
            defaultValue = inputDefaultValue;
        } else if (metaInfo.isDeriveClazz(clz)) {
            maps = drive.get(DERIVE);
            defaultValue = driveDefaultValue;
        } else if (metaInfo.isOutputClazz(clz)) {
            maps = output.get(clz.getSimpleName());
            defaultValue = outputDefaultValue;
        } else {
            return "";
        }
        if (defaultValue == null) {
            defaultValue = new HashMap<>(2);
        }
        Object value;
        if (maps == null || maps.isEmpty()) {
            value = defaultValue.get(field);
        } else {
            value = maps.get(0).get(field);
        }
        return value;
    }

}
