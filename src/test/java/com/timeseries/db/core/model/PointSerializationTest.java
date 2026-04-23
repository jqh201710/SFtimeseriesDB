package com.timeseries.db.core.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Point 序列化/反序列化测试：验证 JSON 键名缩写（方案A）
 */
public class PointSerializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testSerializeShortKeys() throws Exception {
        Point point = new Point();
        point.setMeasurement("cpu_usage");
        point.setTags(new HashMap<>());
        Map<String, Object> fields = new HashMap<>();
        fields.put("value", 85.5);
        point.setFields(fields);
        point.setTimestamp(1775639053596L);

        String json = objectMapper.writeValueAsString(point);

        // 验证使用短键名
        assertTrue(json.contains("\"m\":\"cpu_usage\""), "measurement 应序列化为 m");
        assertTrue(json.contains("\"t\":{}"), "tags 应序列化为 t");
        assertTrue(json.contains("\"f\":"), "fields 应序列化为 f");
        assertTrue(json.contains("\"ts\":1775639053596"), "timestamp 应序列化为 ts");
        // 验证不使用长键名
        assertFalse(json.contains("measurement"), "不应包含长键名 measurement");
        assertFalse(json.contains("timestamp"), "不应包含长键名 timestamp");
    }

    @Test
    public void testDeserializeShortKeys() throws Exception {
        String json = "{\"m\":\"memory_usage\",\"t\":{\"host\":\"server1\"},\"f\":{\"value\":1024},\"ts\":1775639053596}";

        Point point = objectMapper.readValue(json, Point.class);

        assertEquals("memory_usage", point.getMeasurement());
        assertEquals("server1", point.getTags().get("host"));
        assertEquals(1024, point.getFields().get("value"));
        assertEquals(1775639053596L, point.getTimestamp());
    }

    @Test
    public void testSerializeWithTags() throws Exception {
        Point point = new Point();
        point.setMeasurement("disk");
        Map<String, String> tags = new HashMap<>();
        tags.put("host", "192.168.1.1");
        tags.put("region", "cn-north");
        point.setTags(tags);
        point.setFields(new HashMap<>());
        point.setTimestamp(1000L);

        String json = objectMapper.writeValueAsString(point);
        Point parsed = objectMapper.readValue(json, Point.class);

        assertEquals("disk", parsed.getMeasurement());
        assertEquals("192.168.1.1", parsed.getTags().get("host"));
        assertEquals("cn-north", parsed.getTags().get("region"));
    }
}
