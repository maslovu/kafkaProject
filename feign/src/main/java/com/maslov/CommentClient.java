package com.maslov;

import com.maslov.dto.CommentEvent;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;

@FeignClient(name = "commentClient", url = "${services.books-maslov.url}", path = "${services.books-maslov.path}")
public interface CommentClient {

    @PostMapping("/comment/batch")
    void sendCommentsBatch(@RequestBody List<CommentEvent> comments);
}
