package ai.pipestream.engine.mapping.util;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class NoOpProcessingBufferTest {

    @TempDir
    Path tempDir; // Using TempDir to ensure clean slate for each test

    private FileSystem fileSystem;
    private Path mockSaveLocation; // Represents the default save location for some constructors

    @BeforeEach
    void setup() throws IOException {
        // Create a new in-memory file system for each test
        fileSystem = com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder.newEmpty().build();
        // Define a "default" save location within this mock file system
        mockSaveLocation = fileSystem.getPath("/test_output");
        // Ensure this directory exists in the mock FS before tests, mimicking default behavior
        Files.createDirectories(mockSaveLocation);
    }

    private PipeDoc createTestPipeDoc(String id, String title) {
        return PipeDoc.newBuilder()
                .setDocId(id)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle(title)
                .setBody("Test content for " + title)
                .setSourceUri("/test/path/" + id + ".txt")
                .setSourceMimeType("text/plain")
                .build()).build();
    }

    @Test
    void add_doesNothing() {
        NoOpProcessingBuffer<PipeDoc> buffer = new NoOpProcessingBuffer<>();
        buffer.add(createTestPipeDoc("test", "title"));
        assertEquals(0, buffer.size());
    }

    @Test
    void saveToDisk_doesNothing_withDefaultParams() throws Exception {
        NoOpProcessingBuffer<PipeDoc> buffer = new NoOpProcessingBuffer<>();
        buffer.add(createTestPipeDoc("test", "title"));
        buffer.saveToDisk();

        // The NoOp buffer should not create any files.
        // We're checking the *contents* of the root of the mock file system
        // for any regular files that would have been written.
        assertTrue(Files.list(fileSystem.getPath("./")).filter(Files::isRegularFile).collect(Collectors.toList()).isEmpty());
        // Also check if the default location we explicitly created for other tests is still empty of files.
        assertTrue(Files.list(mockSaveLocation).filter(Files::isRegularFile).collect(Collectors.toList()).isEmpty());
    }

    @Test
    void saveToDisk_doesNothing_withPrefixAndPrecision() throws Exception {
        NoOpProcessingBuffer<PipeDoc> buffer = new NoOpProcessingBuffer<>();
        buffer.add(createTestPipeDoc("test", "title"));
        buffer.saveToDisk("custom_prefix", 5);

        assertTrue(Files.list(fileSystem.getPath("./")).filter(Files::isRegularFile).collect(Collectors.toList()).isEmpty());
        assertTrue(Files.list(mockSaveLocation).filter(Files::isRegularFile).collect(Collectors.toList()).isEmpty());
    }

    @Test
    void saveToDisk_doesNothing_withLocationPrefixAndPrecision() throws Exception {
        NoOpProcessingBuffer<PipeDoc> buffer = new NoOpProcessingBuffer<>();
        buffer.add(createTestPipeDoc("test", "title"));
        Path specificMockLocation = fileSystem.getPath("/specific_output");
        Files.createDirectories(specificMockLocation); // Create the target directory in mock FS

        buffer.saveToDisk(specificMockLocation, "specific_prefix", 4);

        assertTrue(Files.list(specificMockLocation).filter(Files::isRegularFile).collect(Collectors.toList()).isEmpty());
        assertTrue(Files.list(fileSystem.getPath("./")).filter(Files::isRegularFile).collect(Collectors.toList()).isEmpty());
        assertTrue(Files.list(mockSaveLocation).filter(Files::isRegularFile).collect(Collectors.toList()).isEmpty());
    }

    @Test
    void size_alwaysReturnsZero() {
        NoOpProcessingBuffer<PipeDoc> buffer = new NoOpProcessingBuffer<>();
        buffer.add(createTestPipeDoc("test", "title"));
        assertEquals(0, buffer.size());
    }

    @Test
    void clear_doesNothing() {
        NoOpProcessingBuffer<PipeDoc> buffer = new NoOpProcessingBuffer<>();
        buffer.add(createTestPipeDoc("test", "title"));
        buffer.clear();
        assertEquals(0, buffer.size());
    }

    @Test
    void snapshot_alwaysReturnsEmptyList() {
        NoOpProcessingBuffer<PipeDoc> buffer = new NoOpProcessingBuffer<>();
        buffer.add(createTestPipeDoc("test", "title"));
        List<PipeDoc> snapshot = buffer.snapshot();
        assertNotNull(snapshot);
        assertTrue(snapshot.isEmpty());
        // Specifically assert that it's the singleton empty list, as implemented in NoOpProcessingBuffer
        assertSame(Collections.emptyList(), snapshot);
    }

    @Test
    void saveOnShutdown_doesNothing() throws IOException {
        NoOpProcessingBuffer<PipeDoc> buffer = new NoOpProcessingBuffer<>();
        buffer.add(createTestPipeDoc("test", "title"));
        buffer.saveOnShutdown();

        assertTrue(Files.list(fileSystem.getPath("./")).filter(Files::isRegularFile).collect(Collectors.toList()).isEmpty());
        assertTrue(Files.list(mockSaveLocation).filter(Files::isRegularFile).collect(Collectors.toList()).isEmpty());
    }
}