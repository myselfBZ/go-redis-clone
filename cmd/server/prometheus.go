package main

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    // Counter for total commands processed
    commandsProcessed = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "redis_commands_total",
            Help: "Total number of Redis commands processed",
        },
        []string{"command"}, // Labels: GET, SET, etc.
    )

    // Gauge for current connections
    activeConnections = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "redis_active_connections",
            Help: "Number of active client connections",
        },
    )

    // Histogram for command duration
    commandDuration = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "redis_command_duration_seconds",
            Help: "Command execution duration in seconds",
            Buckets: prometheus.DefBuckets,
        },
        []string{"command"},
    )

    // Gauge for memory usage
    memoryUsage = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "redis_memory_bytes",
            Help: "Current memory usage in bytes",
        },
    )

    // Gauge for keys stored
    keysStored = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "redis_keys_total",
            Help: "Total number of keys stored",
        },
    )
)
