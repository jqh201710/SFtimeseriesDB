package com.timeseries.db.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 时序库极简API调用工具类
 * 封装/api/simple下所有接口，对外提供标准化调用方法
 * 支持自定义服务地址、超时时间等配置
 */
public class SimpleTimeSeriesApiClient {
    // ===================== 可配置项 =====================
    private final String baseUrl; // 服务基础地址
    private final int connectTimeout; // 连接超时时间（毫秒）
    private final int readTimeout; // 读取超时时间（毫秒）
    // JSON解析器（线程安全，全局复用）
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // ===================== 构造方法（支持自定义配置） =====================
    /**
     * 默认构造器：使用默认配置（http://localhost:11002/timeseries/api/simple，超时30秒）
     */
    public SimpleTimeSeriesApiClient() {
        this("http://localhost:11002/api/timeseries", 30000, 30000);
    }

    /**
     * 自定义构造器：指定服务地址，使用默认超时
     * @param baseUrl 服务基础地址（如：http://192.168.1.100:8080/api/simple）
     */
    public SimpleTimeSeriesApiClient(String baseUrl) {
        this(baseUrl, 30000, 30000);
    }

    /**
     * 全量自定义构造器
     * @param baseUrl 服务基础地址
     * @param connectTimeout 连接超时（毫秒）
     * @param readTimeout 读取超时（毫秒）
     */
    public SimpleTimeSeriesApiClient(String baseUrl, int connectTimeout, int readTimeout) {
        this.baseUrl = baseUrl;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
    }

    // ===================== 对外暴露的核心方法 =====================
    /**
     * 1. 极简写入：单条时序数据
     * @param measurement 指标名（如cpu_usage）
     * @param value 指标值（如0.85、1024）
     * @param timestamp 时间戳（毫秒）
     * @return 接口响应结果（success/fail）
     * @throws Exception 调用异常（可根据业务封装为自定义异常）
     */
    public String write(String measurement, Object value, long timestamp) throws Exception {
        Map<String, Object> requestMap = new HashMap<>(3);
        requestMap.put("measurement", measurement);
        requestMap.put("value", value);
        requestMap.put("timestamp", timestamp);
        String requestJson = OBJECT_MAPPER.writeValueAsString(requestMap);
        return doPost("/write", requestJson);
    }

    /**
     * 2. 极简批量写入：多条时序数据
     * @param dataList 数据列表（每个元素包含measurement/value/timestamp）
     * @return 接口响应结果（success/fail）
     * @throws Exception 调用异常
     */
    public String batchWrite(List<SimpleData> dataList) throws Exception {
        List<Map<String, Object>> requestList = new ArrayList<>(dataList.size());
        for (SimpleData data : dataList) {
            Map<String, Object> item = new HashMap<>(3);
            item.put("measurement", data.getMeasurement());
            item.put("value", data.getValue());
            item.put("timestamp", data.getTimestamp());
            requestList.add(item);
        }
        String requestJson = OBJECT_MAPPER.writeValueAsString(requestList);
        return doPost("/batch-write", requestJson);
    }

    /**
     * 3. 单指标实时值查询：查询指定时间点的数值
     * @param measurement 指标名
     * @param targetTime 目标时间戳（毫秒）
     * @return 实时值数据点（包含timestamp和value）
     * @throws Exception 调用异常
     */
    public SimpleDataPoint querySinglePoint(String measurement, long targetTime) throws Exception {
        String params = String.format("measurement=%s&targetTime=%d",
                URLEncoder.encode(measurement, StandardCharsets.UTF_8.name()), targetTime);
        String responseJson = doGet("/query/point?" + params);
        Map<String, Object> responseMap = OBJECT_MAPPER.readValue(responseJson, new TypeReference<Map<String, Object>>() {});
        Map<String, Object> pointMap = (Map<String, Object>) responseMap.get("singlePoint");
        return convertToSimpleDataPoint(pointMap);
    }

    /**
     * 4. 单指标范围查询：查询指定时间范围内的所有数据
     * @param measurement 指标名
     * @param startTime 开始时间戳（毫秒）
     * @param endTime 结束时间戳（毫秒）
     * @return 数据点列表
     * @throws Exception 调用异常
     */
    public List<SimpleDataPoint> queryRange(String measurement, long startTime, long endTime) throws Exception {
        String params = String.format("measurement=%s&start=%d&end=%d",
                URLEncoder.encode(measurement, StandardCharsets.UTF_8.name()), startTime, endTime);
        String responseJson = doGet("/query/range?" + params);
        Map<String, Object> responseMap = OBJECT_MAPPER.readValue(responseJson, new TypeReference<Map<String, Object>>() {});
        List<Map<String, Object>> pointMapList = (List<Map<String, Object>>) responseMap.get("dataPoints");
        return convertToSimpleDataPointList(pointMapList);
    }

    /**
     * 5. 多指标范围查询：一次性查询多个指标的时间范围数据
     * @param measurements 指标名列表（如cpu_usage,memory_usage）
     * @param startTime 开始时间戳（毫秒）
     * @param endTime 结束时间戳（毫秒）
     * @return 按指标名分组的数据集（key=指标名，value=数据点列表）
     * @throws Exception 调用异常
     */
    public Map<String, List<SimpleDataPoint>> queryMultiMetric(List<String> measurements, long startTime, long endTime) throws Exception {
        String measurementStr = String.join(",", measurements);
        String params = String.format("measurements=%s&start=%d&end=%d",
                URLEncoder.encode(measurementStr, StandardCharsets.UTF_8.name()), startTime, endTime);
        String responseJson = doGet("/query/multi?" + params);
        Map<String, Object> responseMap = OBJECT_MAPPER.readValue(responseJson, new TypeReference<Map<String, Object>>() {});
        Map<String, List<SimpleDataPoint>> resultMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> metricDataMap = (Map<String, List<Map<String, Object>>>) responseMap.get("metricData");
        if (metricDataMap != null) {
            for (Map.Entry<String, List<Map<String, Object>>> entry : metricDataMap.entrySet()) {
                resultMap.put(entry.getKey(), convertToSimpleDataPointList(entry.getValue()));
            }
        }
        return resultMap;
    }

    // ===================== 内部通用方法（私有化，不对外暴露） =====================
    /**
     * 发送POST请求（内部通用）
     */
    private String doPost(String path, String requestJson) throws Exception {
        URL url = new URL(baseUrl + path);
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            conn.setConnectTimeout(connectTimeout);
            conn.setReadTimeout(readTimeout);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = requestJson.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new RuntimeException("POST请求失败，响应码：" + responseCode + "，路径：" + path + "，基础地址：" + baseUrl);
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    response.append(line);
                }
                return response.toString();
            }
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * 发送GET请求（内部通用）
     */
    private String doGet(String path) throws Exception {
        URL url = new URL(baseUrl + path);
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(connectTimeout);
            conn.setReadTimeout(readTimeout);

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new RuntimeException("GET请求失败，响应码：" + responseCode + "，路径：" + path + "，基础地址：" + baseUrl);
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    response.append(line);
                }
                return response.toString();
            }
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * 转换为标准化数据点（内部工具方法）
     */
    private SimpleDataPoint convertToSimpleDataPoint(Map<String, Object> pointMap) {
        if (pointMap == null) {
            return null;
        }
        SimpleDataPoint dataPoint = new SimpleDataPoint();
        dataPoint.setTimestamp(((Number) pointMap.get("timestamp")).longValue());
        dataPoint.setValue(pointMap.get("value"));
        return dataPoint;
    }

    /**
     * 转换为标准化数据点列表（内部工具方法）
     */
    private List<SimpleDataPoint> convertToSimpleDataPointList(List<Map<String, Object>> pointMapList) {
        List<SimpleDataPoint> dataPointList = new ArrayList<>();
        if (pointMapList == null || pointMapList.isEmpty()) {
            return dataPointList;
        }
        for (Map<String, Object> pointMap : pointMapList) {
            dataPointList.add(convertToSimpleDataPoint(pointMap));
        }
        return dataPointList;
    }

    // ===================== 内部数据模型（标准化） =====================
    /**
     * 批量写入数据模型
     * 用于封装批量写入的单条数据
     */
    public static class SimpleData {
        private String measurement;
        private Object value;
        private long timestamp;

        public SimpleData() {}

        public SimpleData(String measurement, Object value, long timestamp) {
            this.measurement = measurement;
            this.value = value;
            this.timestamp = timestamp;
        }

        public String getMeasurement() { return measurement; }
        public void setMeasurement(String measurement) { this.measurement = measurement; }
        public Object getValue() { return value; }
        public void setValue(Object value) { this.value = value; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    /**
     * 数据点模型（标准化响应）
     * 所有查询接口的统一返回数据格式
     */
    public static class SimpleDataPoint {
        private long timestamp;
        private Object value;

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public Object getValue() { return value; }
        public void setValue(Object value) { this.value = value; }

        @Override
        public String toString() {
            return "SimpleDataPoint{" +
                    "timestamp=" + timestamp +
                    ", value=" + value +
                    '}';
        }
    }

    // ===================== 测试用例（main方法） =====================
    public static void main(String[] args) {
        // 1. 创建客户端实例（使用默认构造器，对应地址：http://localhost:11002/timeseries/api/simple）
        SimpleTimeSeriesApiClient client = new SimpleTimeSeriesApiClient();
        // 若需要修改服务地址，使用自定义构造器：
        // SimpleTimeSeriesApiClient client = new SimpleTimeSeriesApiClient("http://192.168.1.100:11002/timeseries/api/simple");

        // 测试用公共时间戳（2025-06-01 00:00:00 及后续5分钟，方便查询）
        long baseTs = 1748889600000L; // 基准时间戳
        long ts1 = baseTs;            // 2025-06-01 00:00:00
        long ts2 = baseTs + 300000L;  // 2025-06-01 00:05:00
        long ts3 = baseTs + 600000L;  // 2025-06-01 00:10:00

        try {
            System.out.println("===================== 开始测试时序库API =====================");
            // 测试1：单条写入（CPU使用率）
            System.out.println("\n【测试1：单条写入】");
            String writeRes = client.write("cpu_usage", 0.85, ts1);
            System.out.println("写入结果：" + writeRes + " | 写入数据：cpu_usage, 0.85, " + ts1);

            // 测试2：批量写入（CPU、内存、磁盘指标，多时间戳）
            System.out.println("\n【测试2：批量写入】");
            List<SimpleData> batchData = new ArrayList<>();
            batchData.add(new SimpleData("cpu_usage", 0.82, ts2));
            batchData.add(new SimpleData("cpu_usage", 0.88, ts3));
            batchData.add(new SimpleData("memory_usage", 8560, ts1)); // 内存使用量：8560MB
            batchData.add(new SimpleData("memory_usage", 8620, ts2));
            batchData.add(new SimpleData("disk_usage", 68.5, ts1));   // 磁盘使用率：68.5%
            batchData.add(new SimpleData("disk_usage", 68.8, ts3));
            String batchWriteRes = client.batchWrite(batchData);
            System.out.println("批量写入结果：" + batchWriteRes + " | 写入数据条数：" + batchData.size());

            // 测试3：单指标实时值查询（查询cpu_usage在ts1时间点的值）
            System.out.println("\n【测试3：单指标实时值查询】");
            SimpleDataPoint cpuSinglePoint = client.querySinglePoint("cpu_usage", ts1);
            if (cpuSinglePoint != null) {
                System.out.println("查询指标：cpu_usage | 目标时间戳：" + ts1 + " | 查询结果：" + cpuSinglePoint);
            } else {
                System.out.println("查询指标：cpu_usage | 目标时间戳：" + ts1 + " | 查询结果：无数据");
            }

            // 测试4：单指标范围查询（查询cpu_usage在ts1-ts3范围内的所有数据）
            System.out.println("\n【测试4：单指标范围查询】");
            List<SimpleDataPoint> cpuRangeList = client.queryRange("cpu_usage", ts1, ts3);
            System.out.println("查询指标：cpu_usage | 时间范围：" + ts1 + " ~ " + ts3 + " | 查询到数据条数：" + cpuRangeList.size());
            for (SimpleDataPoint point : cpuRangeList) {
                System.out.println("  - " + point);
            }

            // 测试5：多指标范围查询（同时查询cpu_usage、memory_usage、disk_usage在ts1-ts3范围内的数据）
            System.out.println("\n【测试5：多指标范围查询】");
            List<String> multiMetrics = new ArrayList<>();
            multiMetrics.add("cpu_usage");
            multiMetrics.add("memory_usage");
            multiMetrics.add("disk_usage");
            Map<String, List<SimpleDataPoint>> multiMetricMap = client.queryMultiMetric(multiMetrics, ts1, ts3);
            System.out.println("查询指标列表：" + multiMetrics + " | 时间范围：" + ts1 + " ~ " + ts3);
            for (Map.Entry<String, List<SimpleDataPoint>> entry : multiMetricMap.entrySet()) {
                System.out.println("  指标[" + entry.getKey() + "]：" + entry.getValue().size() + "条数据");
                for (SimpleDataPoint point : entry.getValue()) {
                    System.out.println("    - " + point);
                }
            }

            System.out.println("\n===================== 所有测试用例执行完成 =====================");

        } catch (Exception e) {
            // 全局异常捕获，打印详细错误信息
            System.err.println("\n===================== 测试执行异常 =====================");
            System.err.println("异常原因：" + e.getMessage());
            System.err.println("异常堆栈：");
            e.printStackTrace();
        }
    }
}