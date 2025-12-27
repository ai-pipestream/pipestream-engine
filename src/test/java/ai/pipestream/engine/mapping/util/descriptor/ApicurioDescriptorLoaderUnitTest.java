package ai.pipestream.engine.mapping.util.descriptor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ApicurioDescriptorLoaderUnitTest {

    @Test
    void builderValidatesRequiredFields() {
        IllegalArgumentException noUrl = assertThrows(IllegalArgumentException.class,
                () -> ApicurioDescriptorLoader.builder().groupId("g").build());
        assertTrue(noUrl.getMessage().toLowerCase().contains("url"));

        IllegalArgumentException noGroup = assertThrows(IllegalArgumentException.class,
                () -> ApicurioDescriptorLoader.builder().registryUrl("http://x").build());
        assertTrue(noGroup.getMessage().toLowerCase().contains("group"));
    }

    @Test
    void placeholderImplementationReportsUnavailable() throws Exception {
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader("http://x", "g", null);
        assertFalse(loader.isAvailable());
        assertEquals("Apicurio Schema Registry", loader.getLoaderType());
        assertThrows(DescriptorLoader.DescriptorLoadException.class, loader::loadDescriptors);
        assertThrows(DescriptorLoader.DescriptorLoadException.class, () -> loader.loadDescriptor("foo.proto"));
    }
}

