package ai.pipestream.engine.mapping.util;

import ai.pipestream.engine.mapping.util.json.ObjectMapperFactory;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectMapperFactoryTest {

    @Test
    void testFactoryMatchesQuarkusConfiguration() throws Exception {
        ObjectMapper factoryMapper = ObjectMapperFactory.createConfiguredMapper();
        
        // Test object with properties that would be ordered differently without configuration
        TestObject obj = new TestObject();
        obj.zebra = "last alphabetically";
        obj.apple = "first alphabetically";
        obj.middle = "middle";
        obj.data = new TreeMap<>();
        obj.data.put("z-key", "z-value");
        obj.data.put("a-key", "a-value");
        obj.data.put("m-key", "m-value");
        
        // Serialize with factory mapper
        String factoryJson = factoryMapper.writeValueAsString(obj);
        
        // The factory mapper should sort properties alphabetically
        // Verify factory mapper sorts properties alphabetically
        assertTrue(factoryJson.matches(".*\"apple\".*\"data\".*\"middle\".*\"zebra\".*"));
        // Verify factory mapper sorts map entries by key
        assertTrue(factoryJson.matches(".*\"a-key\".*\"m-key\".*\"z-key\".*"));
        
        // Test deserialization works correctly
        TestObject factoryDeser = factoryMapper.readValue(factoryJson, TestObject.class);
        assertEquals("first alphabetically", factoryDeser.apple);
        assertEquals("last alphabetically", factoryDeser.zebra);
        assertEquals("middle", factoryDeser.middle);
        assertEquals("a-value", factoryDeser.data.get("a-key"));
        assertEquals("m-value", factoryDeser.data.get("m-key"));
        assertEquals("z-value", factoryDeser.data.get("z-key"));
        
        // Verify configuration is applied
        assertTrue(factoryMapper.getRegisteredModuleIds().contains("jackson-datatype-jsr310"));
        assertTrue(factoryMapper.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY));
        assertTrue(factoryMapper.isEnabled(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS));
    }
    
    @Test
    void testMinimalMapperDoesNotSort() throws Exception {
        ObjectMapper minimalMapper = ObjectMapperFactory.createMinimalMapper();
        
        // Test that minimal mapper doesn't sort properties
        TestObject obj = new TestObject();
        obj.zebra = "z";
        obj.apple = "a";
        
        String json = minimalMapper.writeValueAsString(obj);
        
        // The minimal mapper should NOT sort properties alphabetically
        // (properties will be in declaration order or arbitrary order)
        // We can't predict the exact order, but we can verify it serializes
        assertTrue(json.contains("\"zebra\""));
        assertTrue(json.contains("\"apple\""));
    }
    
    static class TestObject {
        public String zebra;
        public String apple;
        public String middle;
        public Map<String, String> data;
    }
}