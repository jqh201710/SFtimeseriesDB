package com.timeseries.db.controller;

class CleanupRequest {
    private int retentionDays = 30;

    // Getters and Setters
    public int getRetentionDays() { return retentionDays; }
    public void setRetentionDays(int retentionDays) { this.retentionDays = retentionDays; }
}
