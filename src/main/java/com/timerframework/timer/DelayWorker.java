package com.timerframework.timer;


import com.google.gson.reflect.TypeToken;

/**
 * @author xueweidong
 * @date 2020/11/4
 * @description
 */
public interface DelayWorker<T> {

    /**
     * process delayEvent
     *
     * @param delayEvent
     */
    void process(DelayEvent<T> delayEvent);

    /**
     * 转换泛型
     *
     * @return
     */
    default TypeToken<DelayEvent<T>> getTypeToken() {
        return new TypeToken<DelayEvent<T>>() {
            private static final long serialVersionUID = -4809500924811185870L;
        };
    }
}
