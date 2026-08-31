package com.maslov.client;

import com.maslov.dto.CommentRequest;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "", url = "{services.books-maslov.url}", path = "{services.books-maslov.path}")
public interface CommentClient {

    @PostMapping("/{bookId}/comment")
    void createComment(@PathVariable("bookId") String bookId, @RequestBody CommentRequest comment);
}
