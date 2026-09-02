package com.maslov.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CommentEvent {
    @JsonProperty("book_id")
    private Long bookId;
    @JsonProperty("comment")
    private String comment;
}
