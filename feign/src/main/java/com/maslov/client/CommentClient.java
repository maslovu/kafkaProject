package com.maslov.client;

import com.maslov.config.CommentClientConfig;
import com.maslov.dto.CommentEvent;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;

@FeignClient(
        name = "commentClient",
        url = "http://api-app-service:8080/api/books",
        contextId = "commentFeignClient",
        configuration = CommentClientConfig.class)
public interface CommentClient {

    @PostMapping("/comment/batch")
    ResponseEntity<Void> sendCommentsBatch(@RequestBody List<CommentEvent> comments);
}
