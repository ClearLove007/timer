package com.timerframework.timer;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author xueweidong
 * @date 2020/11/4
 * @description
 */
@Data
public class DelayEvent<T> implements Serializable {

    private static final long serialVersionUID = 3803911443732111443L;

    /**
     * id最大长度
     */
    private static final int MAX_ID_LENGTH = 128;

    /**
     * event id
     */
    private String id;

    /**
     * {@link DelayWorker}中的泛型数据
     */
    private T data;

    /**
     * 延迟执行的时间单位
     */
    private TimeUnit timeUnit;

    /**
     * 延迟执行的时间
     */
    private long delay;

    /**
     * 工作的class 继承{@link DelayWorker}
     */
    private String workerClazz;

    /**
     * 创建时间
     */
    private long createTime;

    /**
     * 执行间隔
     */
    private int maxExecuteInSecond;

    public DelayEvent(Class<? extends DelayWorker<T>> clazz, T data, TimeUnit timeUnit, long delay) {
        this(clazz, UUID.randomUUID().toString(), data, timeUnit, delay);
    }

    public DelayEvent(Class<? extends DelayWorker<T>> clazz, String id, T data, TimeUnit timeUnit, long delay) {
        this.maxExecuteInSecond = 300;

        assert StringUtils.isNotBlank(id) && id.length() < 128;

        this.id = id;
        this.data = data;
        this.timeUnit = timeUnit;
        this.delay = delay;
        this.workerClazz = clazz.getName();
        this.createTime = System.currentTimeMillis();
    }

    public static <T> DelayEvent<T> of(Class<? extends DelayWorker<T>> workerClass, T data, TimeUnit timeUnit, long delay) {
        return new DelayEvent<T>(workerClass, data, timeUnit, delay);
    }

    public static <T> DelayEvent<T> of(Class<? extends DelayWorker<T>> workerClass, String id, T data, TimeUnit timeUnit, long delay) {
        return new DelayEvent<T>(workerClass, id, data, timeUnit, delay);
    }

}
