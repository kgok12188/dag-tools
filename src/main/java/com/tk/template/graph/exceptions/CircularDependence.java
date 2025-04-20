package com.tk.template.graph.exceptions;

import java.util.List;

public class CircularDependence extends RuntimeException {

    private final List<String> nodes;

    private final String msg;

    public CircularDependence(List<String> nodes) {
        this.nodes = nodes;
        StringBuffer message = new StringBuffer();
        for (String item : nodes) {
            message.append(",").append(item);
        }
        msg = "构成循环依赖的节点:\t" + message.substring(1);
    }

    @Override
    public String getMessage() {
        return msg;
    }

}
