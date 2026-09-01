package com.maslov.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CommentEvent {
    @JsonProperty("book_id")
    private String bookId;
    private CommentRequest comment;
}
