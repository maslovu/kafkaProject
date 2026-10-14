package com.maslov.config;

import feign.Client;
import feign.Logger;
import feign.codec.ErrorDecoder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executor;

@Configuration
@Slf4j
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
                // 1. Получаем базовое исключение Feign для этого статуса
                feign.FeignException exception = feign.FeignException.errorStatus(methodKey, response);

                // 2. Если сервер вернул 5xx или 408 (Таймаут) — это восстановимая ошибка
                if (response.status() >= 500 || response.status() == 408) {
                    log.warn("Feign встретил ошибку {}. Запускаем RetryableException...", response.status());

                    // Обертываем ошибку в RetryableException. Только она триггерит Feign Retryer!
                    return new feign.RetryableException(
                            response.status(),
                            exception.getMessage(),
                            response.request().httpMethod(),
                            (Long) null, // Смещение по времени выполнения ретрая (null — использовать дефолт)
                            response.request()
                    );
                }

                // 3. Для любых 4xx ошибок (включая 400 Bad Request) — возвращаем обычное исключение.
                // Feign НЕ будет делать для него ретраи, и метод сразу уйдет в блок catch нашего сервиса
                return exception;
            }
        };
    }

    @Bean
    public feign.Retryer feignRetryer() {
        // period: 100ms (начальная пауза), maxPeriod: 1s (максимальная пауза), maxAttempts: 3 (всего 3 попытки)
        return new feign.Retryer.Default(100, 1000, 3);
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

    @Bean("httpExecutor")
    public Executor httpExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(50);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("http-scheduler-");
        executor.initialize();
        return executor;
    }
}
