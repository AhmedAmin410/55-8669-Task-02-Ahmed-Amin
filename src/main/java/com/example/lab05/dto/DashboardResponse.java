package com.example.lab05.dto;

import com.example.lab05.model.cassandra.SensorReading;
import com.example.lab05.model.mongo.PurchaseReceipt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public record DashboardResponse(
        String personName,
        Double totalSpent,
        Integer purchaseCount,
        List<PurchaseReceipt> recentPurchases,
        List<Map<String, Object>> friendRecommendations,
        List<String> friendsOfFriends,
        List<SensorReading> recentActivity,
        List<String> youMightAlsoLike,
        boolean servedFromCache
) implements Serializable {}