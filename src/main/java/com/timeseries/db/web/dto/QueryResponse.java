package com.timeseries.db.web.dto;

import com.timeseries.db.core.model.Point;
import lombok.Data;

import java.util.List;

@Data
public class QueryResponse {
    private int count;
    private List<Point> data;
}