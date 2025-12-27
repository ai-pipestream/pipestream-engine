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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract base class for testing ProcessingBuffer implementations.
 * Uses a mock file system for file operations.
 */
public abstract class ProcessingBufferTest {

    @TempDir
    Path tempDir; // Use JUnit 5's TempDir for temporary directory (mocked later)

    protected FileSystem fileSystem; // Will be set to a mock file system
    protected Path mockSaveLocation;

    protected abstract ProcessingBuffer<PipeDoc> createBuffer(int capacity, Path saveLocation);
    protected abstract ProcessingBuffer<PipeDoc> createBuffer(int capacity);

    @BeforeEach
    void setup() throws IOException {
        // MockFileSystem for isolated file I/O testing
        fileSystem = com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder.newEmpty().build();
        mockSaveLocation = fileSystem.getPath("/test_output");
        Files.createDirectories(mockSaveLocation); // Create the root for mock saves
    }

    private PipeDoc createTestPipeDoc(String id, String title) {
        return PipeDoc.newBuilder()
                .setDocId(id)
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title).build())
                .build();
    }

    @Test
    void add_addsMessageToBuffer() {
        ProcessingBuffer<PipeDoc> buffer = createBuffer(3);
        assertEquals(0, buffer.size());
        buffer.add(createTestPipeDoc("doc1", "title1"));
        assertEquals(1, buffer.size());
    }

    @Test
    void add_nullMessageIsIgnored() {
        ProcessingBuffer<PipeDoc> buffer = createBuffer(3);
        buffer.add(null);
        assertEquals(0, buffer.size());
    }

    @Test
    void add_bufferRespectsCapacity() {
        ProcessingBuffer<PipeDoc> buffer = createBuffer(2);
        buffer.add(createTestPipeDoc("doc1", "title1"));
        buffer.add(createTestPipeDoc("doc2", "title2"));
        assertEquals(2, buffer.size());

        buffer.add(createTestPipeDoc("doc3", "title3")); // This should push out doc1
        assertEquals(2, buffer.size());
        assertEquals(List.of(createTestPipeDoc("doc2", "title2"), createTestPipeDoc("doc3", "title3")), buffer.snapshot());
    }

    @Test
    void size_returnsCorrectSize() {
        ProcessingBuffer<PipeDoc> buffer = createBuffer(5);
        buffer.add(createTestPipeDoc("a", "titleA"));
        buffer.add(createTestPipeDoc("b", "titleB"));
        assertEquals(2, buffer.size());
        buffer.clear();
        assertEquals(0, buffer.size());
    }

    @Test
    void clear_removesAllMessages() {
        ProcessingBuffer<PipeDoc> buffer = createBuffer(3);
        buffer.add(createTestPipeDoc("doc1", "title1"));
        buffer.add(createTestPipeDoc("doc2", "title2"));
        buffer.clear();
        assertEquals(0, buffer.size());
        assertTrue(buffer.snapshot().isEmpty());
    }

    @Test
    void snapshot_returnsCopyOfBufferContents() {
        ProcessingBuffer<PipeDoc> buffer = createBuffer(3);
        PipeDoc doc1 = createTestPipeDoc("doc1", "title1");
        PipeDoc doc2 = createTestPipeDoc("doc2", "title2");
        buffer.add(doc1);
        buffer.add(doc2);

        List<PipeDoc> snapshot = buffer.snapshot();
        assertEquals(2, snapshot.size());
        assertEquals(List.of(doc1, doc2), snapshot);

        // Verify snapshot is a copy - adding to buffer doesn't affect snapshot
        buffer.add(createTestPipeDoc("doc3", "title3"));
        assertEquals(2, snapshot.size()); // Original snapshot still has 2
        assertEquals(3, buffer.size()); // Buffer has 3
    }

    // saveToDisk tests - will be more specific in concrete implementations
}