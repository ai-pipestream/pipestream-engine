package ai.pipestream.engine.mapping.util;

import com.google.protobuf.Message;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
 * Factory for creating ProcessingBuffer instances.
 * This factory creates either a ProcessingBufferImpl or a NoOpProcessingBuffer
 * based on the provided configuration.
 */
public class ProcessingBufferFactory {

    /**
     * Default constructor for ProcessingBufferFactory.
     * This class only provides static methods, so the constructor is not used.
     */
    public ProcessingBufferFactory() {
        // No initialization needed
    }

    /**
     * Creates a ProcessingBuffer instance with a specific file system and save parameters.
     *
     * @param <T> the type of protobuf message
     * @param enabled whether to enable the buffer (if false, returns a NoOpProcessingBuffer)
     * @param capacity the capacity of the buffer
     * @param messageClass the class of the protobuf message
     * @param fileSystem the file system to use for saving files
     * @param saveLocation the directory to save the files to
     * @param savePrefix the prefix to use for the file names
     * @param savePrecision the number of digits to use for the file name suffix
     * @return a ProcessingBuffer instance
     */
    public static <T extends Message> ProcessingBuffer<T> createBuffer(
            boolean enabled, int capacity, Class<T> messageClass, FileSystem fileSystem,
            Path saveLocation, String savePrefix, int savePrecision) {
        if (enabled) {
            return new ProcessingBufferImpl<>(capacity, messageClass, fileSystem,
                                             saveLocation, savePrefix, savePrecision);
        } else {
            return new NoOpProcessingBuffer<>();
        }
    }

    /**
     * Creates a ProcessingBuffer instance with the default file system and save parameters.
     *
     * @param <T> the type of protobuf message
     * @param enabled whether to enable the buffer (if false, returns a NoOpProcessingBuffer)
     * @param capacity the capacity of the buffer
     * @param messageClass the class of the protobuf message
     * @param saveLocation the directory to save the files to
     * @param savePrefix the prefix to use for the file names
     * @param savePrecision the number of digits to use for the file name suffix
     * @return a ProcessingBuffer instance
     */
    public static <T extends Message> ProcessingBuffer<T> createBuffer(
            boolean enabled, int capacity, Class<T> messageClass,
            Path saveLocation, String savePrefix, int savePrecision) {
        return createBuffer(enabled, capacity, messageClass, FileSystems.getDefault(),
                           saveLocation, savePrefix, savePrecision);
    }

    /**
     * Creates a ProcessingBuffer instance with a specific file system.
     *
     * @param <T> the type of protobuf message
     * @param enabled whether to enable the buffer (if false, returns a NoOpProcessingBuffer)
     * @param capacity the capacity of the buffer
     * @param messageClass the class of the protobuf message
     * @param fileSystem the file system to use for saving files
     * @return a ProcessingBuffer instance
     */
    public static <T extends Message> ProcessingBuffer<T> createBuffer(
            boolean enabled, int capacity, Class<T> messageClass, FileSystem fileSystem) {
        if (enabled) {
            return new ProcessingBufferImpl<>(capacity, messageClass, fileSystem);
        } else {
            return new NoOpProcessingBuffer<>();
        }
    }

    /**
     * Creates a ProcessingBuffer instance with the default file system.
     *
     * @param <T> the type of protobuf message
     * @param enabled whether to enable the buffer (if false, returns a NoOpProcessingBuffer)
     * @param capacity the capacity of the buffer
     * @param messageClass the class of the protobuf message
     * @return a ProcessingBuffer instance
     */
    public static <T extends Message> ProcessingBuffer<T> createBuffer(
            boolean enabled, int capacity, Class<T> messageClass) {
        return createBuffer(enabled, capacity, messageClass, FileSystems.getDefault());
    }

    /**
     * Creates a ProcessingBuffer instance with default capacity (100) and the default file system.
     *
     * @param <T> the type of protobuf message
     * @param enabled whether to enable the buffer (if false, returns a NoOpProcessingBuffer)
     * @param messageClass the class of the protobuf message
     * @return a ProcessingBuffer instance
     */
    public static <T extends Message> ProcessingBuffer<T> createBuffer(
            boolean enabled, Class<T> messageClass) {
        return createBuffer(enabled, 100, messageClass);
    }

    /**
     * Creates a ProcessingBuffer instance with default capacity (100) and a specific file system.
     *
     * @param <T> the type of protobuf message
     * @param enabled whether to enable the buffer (if false, returns a NoOpProcessingBuffer)
     * @param messageClass the class of the protobuf message
     * @param fileSystem the file system to use for saving files
     * @return a ProcessingBuffer instance
     */
    public static <T extends Message> ProcessingBuffer<T> createBuffer(
            boolean enabled, Class<T> messageClass, FileSystem fileSystem) {
        return createBuffer(enabled, 100, messageClass, fileSystem);
    }
}