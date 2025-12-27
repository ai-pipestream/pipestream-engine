package ai.pipestream.engine.mapping.util;

import ai.pipestream.data.v1.PipeDoc;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystems;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ProcessingBufferFactoryTest {

    @Test
    void createBuffer_enabled_returnsProcessingBufferImpl() {
        ProcessingBuffer<PipeDoc> buffer = ProcessingBufferFactory.createBuffer(
                true, 10, PipeDoc.class, FileSystems.getDefault(), Paths.get("/tmp"), "prefix", 3);
        assertInstanceOf(ProcessingBufferImpl.class, buffer);
    }

    @Test
    void createBuffer_disabled_returnsNoOpProcessingBuffer() {
        ProcessingBuffer<PipeDoc> buffer = ProcessingBufferFactory.createBuffer(
                false, 10, PipeDoc.class, FileSystems.getDefault(), Paths.get("/tmp"), "prefix", 3);

    }

    @Test
    void createBuffer_enabledWithDefaultFS_returnsProcessingBufferImpl() {
        ProcessingBuffer<PipeDoc> buffer = ProcessingBufferFactory.createBuffer(
                true, 10, PipeDoc.class, Paths.get("/tmp"), "prefix", 3);
        assertInstanceOf(ProcessingBufferImpl.class, buffer);
    }

    @Test
    void createBuffer_enabledWithDefaultSaveParams_returnsProcessingBufferImpl() {
        ProcessingBuffer<PipeDoc> buffer = ProcessingBufferFactory.createBuffer(
                true, 10, PipeDoc.class, FileSystems.getDefault());
        assertInstanceOf(ProcessingBufferImpl.class, buffer);
    }

    @Test
    void createBuffer_enabledWithOnlyCapacityAndClass_returnsProcessingBufferImpl() {
        ProcessingBuffer<PipeDoc> buffer = ProcessingBufferFactory.createBuffer(
                true, 10, PipeDoc.class);
        assertInstanceOf(ProcessingBufferImpl.class, buffer);
    }

    @Test
    void createBuffer_enabledWithDefaultCapacityAndClass_returnsProcessingBufferImpl() {
        ProcessingBuffer<PipeDoc> buffer = ProcessingBufferFactory.createBuffer(
                true, PipeDoc.class);
        assertInstanceOf(ProcessingBufferImpl.class, buffer);
    }

    @Test
    void createBuffer_enabledWithDefaultCapacityAndClassAndFS_returnsProcessingBufferImpl() {
        ProcessingBuffer<PipeDoc> buffer = ProcessingBufferFactory.createBuffer(
                true, PipeDoc.class, FileSystems.getDefault());
        assertInstanceOf(ProcessingBufferImpl.class, buffer);
    }
}