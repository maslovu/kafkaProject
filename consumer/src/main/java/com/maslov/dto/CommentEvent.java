package com.maslov.dto;

import lombok.Data;

@Data
public class CommentEvent {
    private String bookId;
    private CommentRequest comment;
}
