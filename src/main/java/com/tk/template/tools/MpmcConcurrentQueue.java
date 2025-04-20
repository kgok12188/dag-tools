package com.tk.template.tools;


/**
 * 环形队列，初始化2的次方幂
 * @param <E>
 */
public class MpmcConcurrentQueue<E> implements ConcurrentQueue<E> {

    protected final int size;

    //	mask    16 - 1 = 15       10000  01111
    final long mask;

    //	a ring buffer representing the queue
    final Cell<E>[] buffer;

    //	head: 头部的计数器
    final ContendedAtomicLong head = new ContendedAtomicLong(0L);

    //	tail: 尾部的计数器
    final ContendedAtomicLong tail = new ContendedAtomicLong(0L);

    @SuppressWarnings("unchecked")
    public MpmcConcurrentQueue(final int capacity) {
        // capacity of at least 2 is assumed
        int c = 2;
        //	capacity = 15
        while (c < capacity) {
            c <<= 1;
        }
        size = c;
        mask = size - 1L;
        buffer = new Cell[size];
        //	缓存的预加载
        for (int i = 0; i < size; i++) {
            buffer[i] = new Cell<>(i);
        }
    }

    @Override
    public boolean offer(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
        long offset;
        do {
            offset = tail.get();
            long diff = buffer[(int) (mask & offset)].seq.get() - offset;
            if (diff == 0) {
                if (tail.compareAndSet(offset, offset + 1)) {
                    Cell<E> cell = buffer[(int) (mask & offset)];
                    cell.entry = e;
                    return true;
                }
            } else {
                return false;
            }
        } while (true);
    }

    @Override
    public E poll() {
        long offset;
        Cell<E> cell;
        long diff;
        do {
            offset = head.get();
            cell = buffer[(int) (mask & offset)];
            diff = cell.seq.get() - offset;
            if (diff == 0 && cell.entry != null) {
                // cell.entry != null 防止，tail.compareAndSet 设置值成功
                // 但是还没有执行 cell.entry = e; 就开始消费数据
                if (head.compareAndSet(offset, offset + 1)) {
                    E entry = cell.entry;
                    cell.entry = null;
                    cell.seq.set(offset + size);
                    return entry;
                }
            } else {
                return null;
            }
        } while (true);
    }

    @Override
    public final E peek() {
        return buffer[(int) (head.get() & mask)].entry;
    }


    @Override
    // drain the whole queue at once
    public int remove(final E[] e) {
        int nRead = 0;
        while (nRead < e.length && !isEmpty()) {
            final E entry = poll();
            if (entry != null) {
                e[nRead++] = entry;
            }
        }
        return nRead;
    }

    @Override
    public final int size() {
        return (int) Math.max((tail.get() - head.get()), 0);
    }

    @Override
    public int capacity() {
        return size;
    }

    @Override
    public final boolean isEmpty() {
        return head.get() == tail.get();
    }

    @Override
    public void clear() {
        while (!isEmpty()) poll();
    }

    @Override
    public final boolean contains(Object o) {
        for (int i = 0; i < size(); i++) {
            final int slot = (int) ((head.get() + i) & mask);
            if (buffer[slot].entry != null && buffer[slot].entry.equals(o)) return true;
        }
        return false;
    }

    protected static final class Cell<R> {

        //	计数器
        final ContendedAtomicLong seq = new ContendedAtomicLong(0L);

        //        public long p1, p2, p3, p4, p5, p6, p7;
        //	实际的内容
        R entry;

//        public long a1, a2, a3, a4, a5, a6, a7, a8;

        Cell(final long s) {
            seq.set(s);
            entry = null;
        }

//        public long sumToAvoidOptimization() {
//            return p1+p2+p3+p4+p5+p6+p7+a1+a2+a3+a4+a5+a6+a7+a8;
//        }

    }

}