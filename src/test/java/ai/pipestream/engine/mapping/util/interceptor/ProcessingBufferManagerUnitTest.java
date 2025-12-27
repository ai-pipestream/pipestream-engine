package ai.pipestream.engine.mapping.util.interceptor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProcessingBufferManagerUnitTest {

    private final ProcessingBufferManager manager = new ProcessingBufferManager();

    @BeforeEach
    void clear() {
        ProcessingBufferInterceptor.clearAllBuffers();
    }

    @Test
    void delegatesToInterceptorStatics() {
        assertNotNull(manager.getActiveBufferKeys());
        assertNotNull(manager.getBufferStatistics());
        assertEquals(0, manager.getTotalCapturedDocuments());

        // No buffers exist yet; should return null/false safely
        assertNull(manager.getBuffer("Nope.method"));
        assertNull(manager.getBuffer("Nope", "method"));
        assertFalse(manager.saveBuffer("Nope.method"));

        // Should not throw
        manager.saveAllBuffers();
        manager.clearAllBuffers();
    }
}

