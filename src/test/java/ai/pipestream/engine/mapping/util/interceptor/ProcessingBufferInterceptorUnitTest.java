package ai.pipestream.engine.mapping.util.interceptor;

import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.v1.PipeDoc;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProcessingBufferInterceptorUnitTest {

    @Test
    void extractsOutputDocFromProcessDataResponse() {
        PipeDoc doc = PipeDoc.newBuilder().setDocId("doc-x").build();
        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setSuccess(true)
                .setOutputDoc(doc)
                .build();

        PipeDoc extracted = ProcessingBufferInterceptor.extractOutputDoc(response);
        assertNotNull(extracted);
        assertEquals("doc-x", extracted.getDocId());
    }

    @Test
    void returnsNullForWrongType() {
        assertNull(ProcessingBufferInterceptor.extractOutputDoc("not a message"));
    }

    @Test
    void returnsNullWhenOutputDocMissing() {
        ProcessDataResponse response = ProcessDataResponse.newBuilder().setSuccess(true).build();
        assertNull(ProcessingBufferInterceptor.extractOutputDoc(response));
    }
}

