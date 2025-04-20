package com.tk.template.tools;


public interface ConcurrentQueue<E> {

    boolean offer(E e);

    E poll();

    E peek();

    int size();

    int capacity();

    boolean isEmpty();

    boolean contains(Object o);

    int remove(E[] e);

    void clear();
    
}