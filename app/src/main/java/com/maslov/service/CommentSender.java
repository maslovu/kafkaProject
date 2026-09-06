package com.maslov.service;

import com.maslov.dto.CommentEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface CommentSender {
    CompletableFuture<Void> send(List<CommentEvent> batch);
}