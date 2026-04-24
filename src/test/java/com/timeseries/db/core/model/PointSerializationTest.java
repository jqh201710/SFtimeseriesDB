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

    @Test
    public void testDeserializeLegacyLongKeys() throws Exception {
        // 旧数据使用长键名，验证 @JsonAlias 能正确兼容
        String legacyJson = "{\"measurement\":\"cpu_usage\",\"tags\":{\"host\":\"srv1\"},\"fields\":{\"value\":99.9},\"timestamp\":1775639053596}";

        Point point = objectMapper.readValue(legacyJson, Point.class);

        assertEquals("cpu_usage", point.getMeasurement());
        assertEquals("srv1", point.getTags().get("host"));
        assertEquals(99.9, point.getFields().get("value"));
        assertEquals(1775639053596L, point.getTimestamp());
    }

    @Test
    public void testDeserializeMixedKeys() throws Exception {
        // 混用新旧键名（理论上不应出现，但验证健壮性）
        String mixedJson = "{\"m\":\"memory\",\"tags\":{},\"f\":{\"v\":1},\"timestamp\":1000}";

        Point point = objectMapper.readValue(mixedJson, Point.class);

        assertEquals("memory", point.getMeasurement());
        assertNotNull(point.getTags());
        assertEquals(1, point.getFields().get("v"));
        assertEquals(1000L, point.getTimestamp());
    }

    @Test
    public void testLegacyDataQueryCompatible() throws Exception {
        // 模拟旧数据在查询链路中的兼容性：timestamp 和 fields 不为 null
        String legacyJson = "{\"measurement\":\"legacy_metric\",\"tags\":{},\"fields\":{\"value\":42},\"timestamp\":1775639053596}";
        Point point = objectMapper.readValue(legacyJson, Point.class);

        // 验证查询链路中关键调用不会 NPE
        assertNotNull(point.getFields());
        assertTrue(point.getFields().containsKey("value"));
        assertTrue(point.getTimestamp() > 0);
        assertNotNull(point.getTags());
    }
}
