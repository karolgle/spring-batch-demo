package com.example.springbatchdemo.repositories;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class WatchlistRepository {
    final private JdbcTemplate jdbcTemplate;

    @Autowired
    public WatchlistRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     *  Methods delete all watchlist with the (current job)type and timestamp < current job timestamp
     * @param type - watchlist type, see. WatchlistType enum
     * @param ts - timestamp the job
     */
    public void deleteByTypeAndLessThenTS(String type, long ts) {
        jdbcTemplate.update("DELETE FROM Watchlist WHERE type = ? AND ts < ?", type, ts);
    }
}
