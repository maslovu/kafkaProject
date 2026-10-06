package com.maslov.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommentEvent {
    @JsonProperty("book_id")
    private Long bookId;
    @JsonProperty("comment")
    private String comment;
}
