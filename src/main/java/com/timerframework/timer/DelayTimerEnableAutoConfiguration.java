package com.timerframework.timer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * @author xueweidong
 * @date 2020/11/5
 * @description
 */
@Configuration
@ConditionalOnProperty(prefix = "timer", name = "enable", havingValue = "true")
public class DelayTimerEnableAutoConfiguration {

    @Bean
    public Timer hashedWheelTimer(@Value("${timer.naming:delay_timer_worker}") String timerThreadNaming, @Value("${timer.tickDurationInMs:100}") int tickDuration, @Value("${timer.ticksPerWheel:512}") int ticksPerWheel) {
        return new HashedWheelTimer((new ThreadFactoryBuilder()).setDaemon(true).setNameFormat(timerThreadNaming).build(), (long)tickDuration, TimeUnit.MILLISECONDS, ticksPerWheel);
    }

    @Bean
    public DelayTimerDispatcher delayTimerDispatcher() {
        return new DelayTimerDispatcher();
    }
}
