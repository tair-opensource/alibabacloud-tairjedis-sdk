package com.kvstore.jedis.tairhash.params;

/**
 * @author bodong.ybd
 * @date 2019/12/30
 */
public class ExhgetwithverResult<T> {
    private T value;
    private long ver;

    public ExhgetwithverResult(T value, long ver) {
        this.value = value;
        this.ver = ver;
    }

    public T getValue() {
        return value;
    }

    public long getVer() {
        return ver;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public void setVer(long ver) {
        this.ver = ver;
    }
}
