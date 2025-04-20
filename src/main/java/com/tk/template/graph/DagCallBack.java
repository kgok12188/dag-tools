package com.tk.template.graph;

import java.util.Map;

public interface DagCallBack<T> {

    void onOk(T t);

    void onError(T t, Map<String,Throwable> errors);

}
