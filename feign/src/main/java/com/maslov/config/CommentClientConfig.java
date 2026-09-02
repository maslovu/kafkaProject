package com.maslov.config;

import feign.Client;
import feign.Logger;
import feign.RetryableException;
import feign.codec.DecodeException;
import feign.codec.ErrorDecoder;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

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

                if (response.status() >= 400 && response.status() < 500) {
                    return feign.FeignException.errorStatus(methodKey, response);
                }

                return super.decode(methodKey, response);
            }
        };
    }

    // 3. Таймауты (Критично для стабильности подов в K8s)
    @Bean
    public Client client() {
        return new Client.Default(null, null) {
        };
    }
}