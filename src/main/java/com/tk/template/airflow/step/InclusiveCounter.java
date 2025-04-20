package com.tk.template.airflow.step;

public class InclusiveCounter {

    private int counter;

    public InclusiveCounter(int counter) {
        this.counter = counter;
    }

    public boolean decrement() {
        counter--;
        return counter == 0;
    }

}
