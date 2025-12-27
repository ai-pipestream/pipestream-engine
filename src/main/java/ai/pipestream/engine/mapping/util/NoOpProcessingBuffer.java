package ai.pipestream.engine.mapping.util;

import com.google.protobuf.Message;
import jakarta.annotation.PreDestroy;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * A no-op implementation of the ProcessingBuffer interface.
 * This implementation doesn't actually store or save any messages.
 * It's used in production environments where we don't want to incur the overhead
 * of storing and saving protobuf messages.
 *
 * @param <T> the type of protobuf message
 */
public class NoOpProcessingBuffer<T extends Message> implements ProcessingBuffer<T> {

    /**
     * Default constructor for NoOpProcessingBuffer.
     * Creates an empty buffer that doesn't store any messages.
     */
    public NoOpProcessingBuffer() {
        // No initialization needed
    }

    /**
     * Does nothing.
     *
     * @param message the message to add (ignored)
     */
    @Override
    public void add(T message) {
        // No-op
    }

    /**
     * Does nothing.
     */
    @Override
    public void saveToDisk() {
        // No-op
    }

    @Override
    public void saveToDisk(String fileNamePrefix, int numberPrecision) {
        // No-op
    }

    /**
     * Does nothing.
     *
     * @param location the directory to save the files to (ignored)
     * @param fileNamePrefix the prefix to use for the file names (ignored)
     * @param numberPrecision the number of digits to use for the file name suffix (ignored)
     */
    @Override
    public void saveToDisk(Path location, String fileNamePrefix, int numberPrecision) {
        // No-op
    }

    /**
     * Always returns 0.
     *
     * @return 0
     */
    @Override
    public int size() {
        return 0;
    }

    /**
     * Does nothing.
     */
    @Override
    public void clear() {
        // No-op
    }

    /**
     * Does nothing on shutdown.
     */
    @PreDestroy
    public void saveOnShutdown() {
        // No-op
    }

    /**
     * Always returns an empty list since this is a no-op buffer.
     *
     * @return an empty list
     */
    @Override
    public List<T> snapshot() {
        return Collections.emptyList();
    }
}