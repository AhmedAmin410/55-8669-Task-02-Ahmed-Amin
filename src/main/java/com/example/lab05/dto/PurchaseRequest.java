package com.example.lab05.dto;

import java.util.Map;

public record PurchaseRequest(
        String personName,
        Long productId,
        Integer quantity,
        Map<String, Object> purchaseDetails
) {}