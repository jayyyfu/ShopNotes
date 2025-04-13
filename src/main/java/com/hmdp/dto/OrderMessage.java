package com.hmdp.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderMessage {
    private Long orderId;
    private Long userId;
    private Long voucherId;
    private Long timestamp;
}