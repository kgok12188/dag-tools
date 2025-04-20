package com.tk.template.airflow;

public abstract class Condition implements Comparable<Condition> {

    private final int level;
    private final String next;

    public Condition(int level, String next) {
        this.level = level;
        this.next = next;
    }

    @Override
    public int compareTo(Condition o) {
        return this.level < o.level ? 1 : -1;
    }

    public abstract boolean condition(DataContext dataContext);

    public String getNext() {
        return next;
    }

}
