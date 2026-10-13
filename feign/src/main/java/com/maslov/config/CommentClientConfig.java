package com.maslov.config;

import feign.Client;
import feign.Logger;
import feign.codec.ErrorDecoder;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executor;

@Configuration
@RequiredArgsConstructor
public class CommentClientConfig {

    // 1. Логирование (для дебага в K8s)
    @Bean
    Logger.Level feignLoggerLevel() {
        return Logger.Level.BASIC;
    }

    // 2. Обработка ошибок (Самый важный пункт)
    @Bean
    public ErrorDecoder errorDecoder() {
        return new ErrorDecoder.Default() {
            @Override
            public Exception decode(String methodKey, feign.Response response) {

                // Если сервер вернул 5xx или возникла сетевая ошибка
                if (response.status() >= 500 || response.status() == 408) {
                    // Бросаем RetryableException. Это скажет Spring Retry "попробуй еще раз".
                    return feign.FeignException.errorStatus(methodKey, response);
                }

                if (response.status() >= 400) {
                    return feign.FeignException.errorStatus(methodKey, response);
                }

                return super.decode(methodKey, response);
            }
        };
    }

    // 3. Таймауты (Критично для стабильности подов в K8s)
    @Bean
    public Client client() {
        try {
            SSLContext sslContext = SSLContext.getDefault();
            return new Client.Default(sslContext.getSocketFactory(), null);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to initialize Feign client with default SSL context", e);
        }
    }

    // Выделяем отдельный пул под внешние HTTP-вызовы, чтобы не занимать потоки Tomcat
    @Bean("httpScheduler")
    public Scheduler httpScheduler() {
        return Schedulers.newBoundedElastic(
                50, // количество потоков
                60, // TTL нити в секундах
                "http-scheduler");
    }

    @Bean("httpExecutor")
    public Executor httpExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(50);
        executor.setMaxPoolSize(50);
        executor.setThreadNamePrefix("http-scheduler-");
        executor.initialize();
        return executor;
    }
}
