# 1. 健康检查
curl -X GET "http://localhost:8080/tsdb/api/v1/timeseries/health"

# 2. 写入单个数据点
curl -X POST "http://localhost:8080/tsdb/api/v1/timeseries/write" \
     -H "Content-Type: application/json" \
     -d '{
       "series": "cpu_usage",
       "timestamp": 1677676800000,
       "value": 75.5,
       "tags": {
         "host": "server01",
         "region": "us-east-1"
       }
     }'

# 3. 批量写入
curl -X POST "http://localhost:8080/tsdb/api/v1/timeseries/batch-write" \
     -H "Content-Type: application/json" \
     -d '{
       "series": "memory_usage",
       "points": [
         {
           "timestamp": 1677676800000,
           "value": 45.2,
           "tags": {"host": "server01"}
         },
         {
           "timestamp": 1677676801000,
           "value": 46.8,
           "tags": {"host": "server01"}
         }
       ]
     }'

# 4. 查询数据
curl -X GET "http://localhost:8080/tsdb/api/v1/timeseries/query?series=cpu_usage&startTime=1677676800000&endTime=1677676900000"

# 5. 查询带标签过滤
curl -X GET "http://localhost:8080/tsdb/api/v1/timeseries/query?series=cpu_usage&startTime=1677676800000&endTime=1677676900000&host=server01"

# 6. 查询多个序列
curl -X POST "http://localhost:8080/tsdb/api/v1/timeseries/query-multiple" \
     -H "Content-Type: application/json" \
     -d '{
       "seriesList": ["cpu_usage", "memory_usage"],
       "startTime": 1677676800000,
       "endTime": 1677676900000
     }'

# 7. 获取序列列表
curl -X GET "http://localhost:8080/tsdb/api/v1/timeseries/list"

# 8. 获取序列统计
curl -X GET "http://localhost:8080/tsdb/api/v1/timeseries/stats/cpu_usage"

# 9. 获取数据库统计
curl -X GET "http://localhost:8080/tsdb/api/v1/timeseries/db-stats"