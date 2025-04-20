package com.tk.template.graph;

import java.util.LinkedList;
import java.util.Queue;

class LocalWorkers<T> extends ThreadLocal<Queue<T>> {
    @Override
    protected Queue<T> initialValue() {
        return new LinkedList<>();
    }
}
