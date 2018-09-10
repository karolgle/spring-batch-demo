package com.example.springbatchdemo.model;

public enum WatchlistType {
    DENY,
    PEP,
    AM;

    public static WatchlistType getRandom() {
        return values()[(int) (Math.random() * values().length)];
    }
}
