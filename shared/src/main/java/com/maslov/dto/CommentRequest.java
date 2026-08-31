package com.maslov.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CommentRequest(
        @NotBlank(message = "Текст комментария не может быть пустым")
        @Size(max = 1000, message = "Комментарий слишком длинный (макс. 1000 символов)")
        String text
) {}