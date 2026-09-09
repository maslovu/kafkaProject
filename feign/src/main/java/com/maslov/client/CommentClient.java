package com.maslov.client;

import com.maslov.config.CommentClientConfig;
import com.maslov.dto.CommentEvent;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;

@FeignClient(
        name = "${feign.client.config.commentClient.name}",
        url = "${feign.client.config.commentClient.url}",
        contextId = "commentFeignClient",
        configuration = CommentClientConfig.class)
public interface CommentClient {

    @PostMapping(value = "/comment/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<Void> sendCommentsBatch(@RequestBody List<CommentEvent> comments);
}
