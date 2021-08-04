package com.aliyun.tair.tairai.results;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TairAIKnngetResult {
    long num;
    List<TairAIKnngetItem> items;

    public TairAIKnngetResult(List obj) {
        items = new ArrayList<>();
        num = obj.size();

        Iterator<List> it = obj.iterator();
        while (it.hasNext()) {
            List item = it.next();
            if (item.size() != 2) {
                throw new IllegalArgumentException(String.format("invalid response for knnget: %s",obj));
            }
            this.append(((Number) item.get(0)).longValue(), ((Number) item.get(1)).longValue());
        }
    }

    private void append(long id, long score) {
        items.add(new TairAIKnngetItem(id, score));
    }

    public TairAIKnngetItem getVector(int index) {
        return items.get(index);
    }

    public long getNum() {
        return num;
    }

    @Override
    public String toString() {
        String ret = "";
        Iterator<TairAIKnngetItem> it = items.iterator();
        while (it.hasNext()) {
            TairAIKnngetItem item = (TairAIKnngetItem) it.next();
            ret += item.toString();
        }
        return ret;
    }


    public static class TairAIKnngetItem {
        private long id;
        private long score;
        public TairAIKnngetItem(long id, long score) {
            this.id = id;
            this.score = score;
        }

        public long getId() {
            return id;
        }

        public long getScore() {
            return score;
        }

        @Override
        public String toString() {
            return "id =" + id +
                    ", score =" + score +
                    ";"
                    ;
        }
    }
}
