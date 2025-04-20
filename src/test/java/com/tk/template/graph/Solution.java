package com.tk.template.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

class Solution {


    private class Item {

        public final Integer current;
        public final Set<Integer> nextItems = new HashSet<>();
        public final Set<Integer> beforeItems = new HashSet<>();

        public Item(Integer current) {
            this.current = current;
        }

        public void addNext(Integer next) {
            nextItems.add(next);
        }

        public void addBefore(Integer before) {
            beforeItems.add(before);
        }

        @Override
        public boolean equals(Object obj) {
            return current.equals(obj);
        }

        @Override
        public int hashCode() {
            return current.hashCode();
        }

        public Set<Integer> getNextItems() {
            return nextItems;
        }

        public Set<Integer> getBeforeItems() {
            return beforeItems;
        }
    }

    public int[] findOrder(int numCourses, int[][] prerequisites) {
        Set<Integer> courses = new HashSet<>();
        HashMap<Integer, Item> itemMap = new HashMap<>();
        for (int i = 0; i < numCourses; i++) {
            courses.add(i);
        }
        for (int i = 0; i < prerequisites.length; i++) {
            courses.remove(prerequisites[i][0]);
            Item item = itemMap.get(prerequisites[i][0]);
            if (item == null) {
                item = new Item(prerequisites[i][0]);
                itemMap.put(prerequisites[i][0], item);
            }
            item.addBefore(prerequisites[i][1]);
            //==========
            item = itemMap.get(prerequisites[i][1]);
            if (item == null) {
                item = new Item(prerequisites[i][1]);
                itemMap.put(prerequisites[i][1], item);
            }
            item.addNext(prerequisites[i][0]);
        }
        LinkedList<Integer> queue = new LinkedList<>();
        queue.addAll(courses);
        HashSet<Integer> result = new HashSet<>();
        int[] orderResult = new int[numCourses];
        int index = 0;
        while (!queue.isEmpty()) {
            Integer c = queue.poll();
            result.add(c);
            orderResult[index++] = c;
            Item item = itemMap.get(c);
            if (item != null) {
                Set<Integer> nextItems = item.getNextItems();
                for (Integer next : nextItems) {
                    Item before = itemMap.get(next);
                    Set<Integer> beforeItems = before.getBeforeItems();
                    boolean flag = true;
                    for (Integer bI : beforeItems) {
                        if (!result.contains(bI)) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        queue.add(next);
                    }
                }
            }
        }

        return orderResult;
    }


    public static void main(String[] args) {
        System.out.println(new Solution().findOrder(2, new int[][]{
                new int[]{0, 1},
                new int[]{1, 2},
                new int[]{2, 0},
        }));

    }

}