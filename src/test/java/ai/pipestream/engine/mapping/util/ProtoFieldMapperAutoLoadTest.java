package ai.pipestream.engine.mapping.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ProtoFieldMapper auto-loading functionality.
 */
public class ProtoFieldMapperAutoLoadTest {

    @Test
    void testWithAutoLoad() {
        ProtoFieldMapper mapper = ProtoFieldMapperImpl.withAutoLoad();
        assertNotNull(mapper);
        assertNotNull(mapper.getDescriptorRegistry());
        assertNotNull(mapper.getAnyHandler());

        // Verify well-known types are available
        DescriptorRegistry registry = mapper.getDescriptorRegistry();
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Struct"));
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Any"));
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Timestamp"));

        // Should have at least the well-known types
        assertTrue(registry.size() > 0);
    }

    @Test
    void testDefaultConstructor() {
        ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl();
        assertNotNull(mapper);
        assertNotNull(mapper.getDescriptorRegistry());

        // Default constructor should have well-known types but not auto-load
        DescriptorRegistry registry = mapper.getDescriptorRegistry();
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Struct"));
    }

    @Test
    void testCustomRegistryConstructor() {
        DescriptorRegistry customRegistry = new DescriptorRegistry(true);
        ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl(customRegistry);

        assertNotNull(mapper);
        assertSame(customRegistry, mapper.getDescriptorRegistry());
    }

    @Test
    void testDescriptorRegistryAccessors() {
        ProtoFieldMapper mapper = ProtoFieldMapperImpl.withAutoLoad();

        DescriptorRegistry registry = mapper.getDescriptorRegistry();
        assertNotNull(registry);

        AnyHandler anyHandler = mapper.getAnyHandler();
        assertNotNull(anyHandler);

        // AnyHandler should use the same registry
        assertSame(registry, anyHandler.getDescriptorRegistry());
    }
}
