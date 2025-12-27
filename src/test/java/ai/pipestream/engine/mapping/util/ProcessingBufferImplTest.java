package ai.pipestream.engine.mapping.util;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class ProcessingBufferImplTest extends ProcessingBufferTest {

    @Override
    protected ProcessingBuffer<PipeDoc> createBuffer(int capacity, Path saveLocation) {
        return new ProcessingBufferImpl<>(capacity, PipeDoc.class, fileSystem, saveLocation, "test_prefix", 3);
    }

    @Override
    protected ProcessingBuffer<PipeDoc> createBuffer(int capacity) {
        return new ProcessingBufferImpl<>(capacity, PipeDoc.class, fileSystem);
    }

    private PipeDoc createTestPipeDoc(String id, String title) {
        return PipeDoc.newBuilder()
                .setDocId(id)
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title).build())
                .build();
    }

    @Test
    void saveToDisk_savesMessagesToDefaultLocation() throws IOException {
        ProcessingBuffer<PipeDoc> buffer = new ProcessingBufferImpl<>(3, PipeDoc.class, fileSystem);
        buffer.add(createTestPipeDoc("doc1", "title1"));
        buffer.add(createTestPipeDoc("doc2", "title2"));

        buffer.saveToDisk(); // Uses default path './' which maps to mockFS root

        List<Path> savedFiles = Files.list(fileSystem.getPath("./"))
                                     .filter(Files::isRegularFile)
                                     .collect(Collectors.toList());

        assertEquals(2, savedFiles.size());
        List<String> fileNames = savedFiles.stream().map(Path::getFileName).map(Path::toString).sorted().collect(Collectors.toList());
        assertEquals(List.of("protobuf-000.bin", "protobuf-001.bin"), fileNames);

        // Verify content
        PipeDoc loadedDoc = PipeDoc.parseFrom(Files.readAllBytes(fileSystem.getPath("./protobuf-000.bin")));
        assertEquals(createTestPipeDoc("doc1", "title1"), loadedDoc);
    }

    @Test
    void saveToDisk_savesMessagesToSpecifiedLocation() throws IOException {
        ProcessingBuffer<PipeDoc> buffer = createBuffer(3, mockSaveLocation);
        buffer.add(createTestPipeDoc("docA", "titleA"));
        buffer.add(createTestPipeDoc("docB", "titleB"));

        buffer.saveToDisk(mockSaveLocation, "custom_prefix", 4);

        List<Path> savedFiles = Files.list(mockSaveLocation)
                                     .filter(Files::isRegularFile)
                                     .toList();

        assertEquals(2, savedFiles.size());
        List<String> fileNames = savedFiles.stream().map(Path::getFileName).map(Path::toString).sorted().collect(Collectors.toList());
        assertEquals(List.of("custom_prefix-0000.bin", "custom_prefix-0001.bin"), fileNames);
    }

    @Test
    void saveToDisk_createsDirectoryIfNotExists() throws IOException {
        Path nonExistentDir = mockSaveLocation.resolve("new_dir");
        ProcessingBuffer<PipeDoc> buffer = createBuffer(1, nonExistentDir);
        buffer.add(createTestPipeDoc("doc", "title"));

        assertFalse(Files.exists(nonExistentDir));
        buffer.saveToDisk(nonExistentDir, "test", 1);
        assertTrue(Files.exists(nonExistentDir));
        assertEquals(1, Files.list(nonExistentDir).count());
    }

    @Test
    void saveOnShutdown_savesIfBufferNotEmpty() throws IOException {
        ProcessingBufferImpl<PipeDoc> buffer = (ProcessingBufferImpl<PipeDoc>) createBuffer(2, mockSaveLocation);
        buffer.add(createTestPipeDoc("shutdown", "title"));

        // Directly call the PreDestroy method for testing purposes
        buffer.saveOnShutdown();

        List<Path> savedFiles = Files.list(mockSaveLocation)
                                     .filter(Files::isRegularFile)
                                     .collect(Collectors.toList());
        assertEquals(1, savedFiles.size());
        assertTrue(savedFiles.get(0).getFileName().toString().startsWith("test_prefix-"));
    }

    @Test
    void saveOnShutdown_doesNothingIfBufferEmpty() throws IOException {
        ProcessingBufferImpl<PipeDoc> buffer = (ProcessingBufferImpl<PipeDoc>) createBuffer(2, mockSaveLocation);
        // Buffer is empty

        buffer.saveOnShutdown();

        assertEquals(0, Files.list(mockSaveLocation).count()); // No files should be saved
    }

    @Test
    void add_clonesMessage() {
        ProcessingBuffer<PipeDoc> buffer = createBuffer(1);
        PipeDoc original = PipeDoc.newBuilder().setDocId("original").build();
        buffer.add(original);

        // Modify the original message AFTER adding it to the buffer
        PipeDoc modifiedOriginal = original.toBuilder().setDocId("modified").build();
        // Since original is immutable, this creates a new instance.
        // We need to ensure the buffer copied the original's state.

        PipeDoc messageInBuffer = buffer.snapshot().get(0);
        assertEquals("original", messageInBuffer.getDocId()); // Should still be "original"
        assertNotSame(original, messageInBuffer); // Should be a clone
    }

}