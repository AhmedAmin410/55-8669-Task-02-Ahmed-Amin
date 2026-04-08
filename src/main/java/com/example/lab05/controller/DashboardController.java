package com.example.lab05.controller;

import com.example.lab05.dto.DashboardResponse;
import com.example.lab05.service.DashboardService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/55-8669/dashboard")
public class DashboardController {

    private final DashboardService dashboardService;

    public DashboardController(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    @GetMapping("/{personName}")
    public DashboardResponse getDashboard(@PathVariable String personName) {
        return dashboardService.getDashboard(personName);
    }
}