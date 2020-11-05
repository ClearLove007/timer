package com.timerframework.timer;

import com.google.common.collect.Lists;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.beans.ConstructorProperties;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * @author xueweidong
 * @date 2020/11/4
 * @description
 */
@Component
@Slf4j
@SuppressWarnings("all")
public class DelayTimerDispatcher {

    @Resource
    private Timer timer;

    private Map<String, DelayWorker<?>> workerMap;

    @Autowired
    private ApplicationContext applicationContext;

    @Value("${spring.application.name}")
    private String app;

    @Autowired
    private RedisTemplate redisTemplate;

    /**
     * redis key
     */
    public static String EVENT_KEY = "delay-event-key";

    /**
     * 最小睡眠时间 ms
     */
    private static final long MIN_SLEEP_TIME_IN_MS = 1L;

    /**
     * 最大睡眠时间 ms
     */
    private static final long MAX_SLEEP_TIME_IN_MS = 3000L;

    /**
     *
     */
    private static final long OVER_TIME_DELAY = 100L;

    /**
     * 最大恢复重试次数
     */
    private static final int MAX_RESTORE_RETRY = 5;

    /**
     * redis批量大小
     */
    private static final int REDIS_BATCH_SIZE = 100;

    private final Gson gson;

    private final ExecutorService restoreExecutorService;

    /**
     * 构造方法
     */
    public DelayTimerDispatcher() {
        gson = (new GsonBuilder()).setDateFormat("yyyy-MM-dd HH:mm:ss").setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
        restoreExecutorService = Executors.newSingleThreadExecutor();
    }

    /**
     * 提交event
     *
     * @param event
     * @param <T>
     * @return
     */
    public <T> String submit(DelayEvent<T> event) {
        try {
            log.debug("New event received {}", event);
            redisTemplate.opsForHash().put(EVENT_KEY, event.getId(), gson.toJson(event));
            timer.newTimeout(new TimerTaskWorker(event.getId()), event.getDelay(), event.getTimeUnit());
        } catch (Exception var3) {
            log.error("Error while submit task :" + event, var3);
            return null;
        }

        return event.getId();
    }

    /**
     * 恢复事件
     *
     * @param eventIds
     * @param <T>
     */
    protected <T> void restoreEvents(Collection<String> eventIds) {
        List<List<String>> eventIdsGroup = Lists.partition(Lists.newArrayList(eventIds), 100);
        eventIdsGroup.forEach((eIds) -> {
            int i = 0;

            while (i < 5) {
                try {
                    List<String> eventStrings = redisTemplate.opsForHash().multiGet(EVENT_KEY, eIds);
                    eventStrings.parallelStream().filter((s) -> {
                        return s != null;
                    }).forEach((e) -> {
                        DelayEvent<?> tmpEvent = (DelayEvent) gson.fromJson(e, DelayEvent.class);
                        if (workerMap.containsKey(tmpEvent.getWorkerClazz())) {
                            DelayEvent<?> event = (DelayEvent) gson.fromJson(e, ((DelayWorker) workerMap.get(tmpEvent.getWorkerClazz())).getTypeToken().getType());
                            long newDelay = event.getCreateTime() + event.getTimeUnit().toMillis(event.getDelay()) - System.currentTimeMillis();
                            log.info("Restore event {} with new delay {} ms", event, newDelay);
                            event.setDelay(newDelay > 0L ? newDelay : 100L);
                            event.setTimeUnit(TimeUnit.MILLISECONDS);
                            event.setCreateTime(System.currentTimeMillis());
                            submit(event);
                        } else {
                            log.error("Can't find worker with class {}, all avaiable worker class [{}]", tmpEvent.getWorkerClazz(), workerMap.keySet());
                        }

                    });
                    break;
                } catch (Exception var6) {
                    log.error("Can't restore events with : " + eIds, var6);
                    log.warn("Cant' restore {} with retry {} fail {}", new Object[]{eIds, i, var6.getMessage()});

                    try {
                        TimeUnit.MILLISECONDS.sleep(RandomUtils.nextLong(1L, 3000L));
                    } catch (InterruptedException var5) {
                        Thread.currentThread().interrupt();
                    }

                    ++i;
                }
            }

        });
    }

    /**
     * 取消任务
     *
     * @param eventId
     */
    public void cancel(String eventId) {
        redisTemplate.opsForHash().delete(EVENT_KEY, new String[]{eventId});
    }

    @PostConstruct
    public void start() throws Exception {
        log.info("DelayTimer start.");
        workerMap = (Map)applicationContext.getBeansOfType(DelayWorker.class).values().stream().collect(Collectors.toMap((w) -> {
            return w.getClass().getName();
        }, Function.identity()));
        EVENT_KEY = app + EVENT_KEY;
        if (!CollectionUtils.isEmpty(workerMap)) {
            Set<String> eventIds = redisTemplate.opsForHash().keys(EVENT_KEY);
            log.info("Restore events with ids {}", eventIds);
            if (eventIds != null) {
                restoreExecutorService.execute(() -> {
                    restoreEvents(eventIds);
                });
                restoreExecutorService.shutdown();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            gracefulyStop();
        }));
    }

    /**
     * 停止Timer
     */
    public void gracefulyStop() {
        restoreExecutorService.shutdownNow();

        while(!restoreExecutorService.isTerminated()) {
            try {
                restoreExecutorService.awaitTermination(10L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException var2) {
                log.error("Can't gracefuly stop", var2);
            }
        }

        timer.stop();
    }

    /**********************************************************************/
    /**********************************************************************/
    /****************************自定义TimerTask****************************/
    /**********************************************************************/
    /**********************************************************************/

    /**
     * TimerTask
     */
    class TimerTaskWorker implements TimerTask {
        private final String eventId;
        private static final String EVENT_LOCK_PREFIX = "event-lock.";

        /**
         * 任务执行逻辑
         *
         * @param timeout
         * @throws Exception
         */
        @Override
        public void run(Timeout timeout) throws Exception {
            boolean seize = false;
            if (redisTemplate.opsForHash().hasKey(EVENT_KEY, eventId)) {
                log.info("Event with id {} still in queue, try to seize execution right.", eventId);
                String eventJson = (String) redisTemplate.opsForHash().get(EVENT_KEY, eventId);
                DelayEvent<?> event = (DelayEvent) gson.fromJson(eventJson, DelayEvent.class);
                if (workerMap.containsKey(event.getWorkerClazz())) {
                    event = (DelayEvent) gson.fromJson(eventJson, ((DelayWorker) workerMap.get(event.getWorkerClazz())).getTypeToken().getType());
                    seize = seizeEvent(event);
                    if (seize) {
                        ((DelayWorker) workerMap.get(event.getWorkerClazz())).process(event);
                    } else {
                        log.info("Seize event {} fail", event);
                    }
                } else {
                    log.warn("Worker clazz not found for event {}, all avaiable worker class are {}", event, workerMap.keySet());
                }
            } else {
                log.info("Event {} has been cancel", eventId);
                timeout.cancel();
            }

            if (seize) {
                clearEventInfo();
            }

        }

        /**
         * 获取event锁的key前缀
         *
         * @return
         */
        private String getEventLockKey() {
            return "event-lock." + eventId;
        }

        /**
         * 清除event信息
         */
        private void clearEventInfo() {
            redisTemplate.opsForHash().delete(EVENT_KEY, new String[]{eventId});
            redisTemplate.delete(getEventLockKey());
        }

        /**
         * 加锁
         *
         * @param event
         * @return
         */
        private boolean seizeEvent(DelayEvent<?> event) {
            Boolean result = redisTemplate.opsForValue().setIfAbsent(getEventLockKey(), eventId);
            if (result != null && result) {
                log.info("Success seize execution right {}", eventId);
                redisTemplate.expire(getEventLockKey(), event.getMaxExecuteInSecond(), TimeUnit.SECONDS);
                return true;
            } else {
                return false;
            }
        }

        /**
         * 构造方法
         *
         * @param eventId
         */
        @ConstructorProperties({"eventId"})
        public TimerTaskWorker(String eventId) {
            this.eventId = eventId;
        }
    }
}
