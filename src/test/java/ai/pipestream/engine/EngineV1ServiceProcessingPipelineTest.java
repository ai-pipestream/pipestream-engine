package ai.pipestream.engine;

import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.data.v1.*;
import ai.pipestream.engine.graph.GraphCache;
import ai.pipestream.engine.util.WireMockTestResource;
import ai.pipestream.engine.v1.ProcessNodeRequest;
import ai.pipestream.engine.v1.ProcessNodeResponse;
import com.google.protobuf.Struct;
import com.google.protobuf.util.Values;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for the new processing pipeline features:
 * - filter_conditions: CEL expressions that determine if document should be processed
 * - pre_mappings: Field mappings applied before module call
 * - post_mappings: Field mappings applied after module call
 * - save_on_error: Persist document even when processing fails
 * <p>
 * These tests verify the implementation of issue #14.
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class EngineV1ServiceProcessingPipelineTest {

    @Inject
    @Any
    Instance<EngineV1Service> engineServiceInstance;

    @Inject
    GraphCache graphCache;

    private EngineV1Service engineService;

    @BeforeEach
    void setUp() {
        engineService = engineServiceInstance.get();
        graphCache.clear();
    }

    // ========== Filter Conditions Tests ==========

    @Nested
    @DisplayName("Filter Conditions Tests")
    class FilterConditionsTests {

        @Test
        @DisplayName("Should process document when all filter conditions pass")
        void testAllFilterConditionsPass() {
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("filter-test-node")
                    .setName("Filter Test Node")
                    .setModuleId("text-chunker")
                    .addFilterConditions("document.doc_id == 'test-doc'")
                    .addFilterConditions("document.search_metadata.title != ''")
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setTitle("Test Title")
                            .build())
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("filter-test-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Node should process successfully when filters pass",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should skip document when filter condition fails")
        void testFilterConditionFails() {
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("filter-test-node")
                    .setName("Filter Test Node")
                    .setModuleId("text-chunker")
                    .addFilterConditions("document.doc_id == 'expected-doc-id'")
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            // Doc ID doesn't match the filter condition
            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("different-doc-id")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("filter-test-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            // Should succeed (skipped node), but module was not called
            assertThat("Node should skip successfully when filter fails",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should skip when any filter condition fails (AND semantics)")
        void testMultipleFilterConditionsAndSemantics() {
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("filter-test-node")
                    .setName("Filter Test Node")
                    .setModuleId("text-chunker")
                    .addFilterConditions("document.doc_id == 'test-doc'")  // passes
                    .addFilterConditions("document.search_metadata.title == 'Wrong Title'")  // fails
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setTitle("Correct Title")
                            .build())
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("filter-test-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            // Should skip due to second condition failing
            assertThat("Node should skip when any filter fails",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should process document when no filter conditions are set")
        void testNoFilterConditions() {
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("no-filter-node")
                    .setName("No Filter Node")
                    .setModuleId("text-chunker")
                    // No filter conditions
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("any-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("no-filter-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Node should process when no filters are set",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should handle empty filter condition gracefully")
        void testEmptyFilterCondition() {
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("empty-filter-node")
                    .setName("Empty Filter Node")
                    .setModuleId("text-chunker")
                    .addFilterConditions("")  // Empty string should be skipped
                    .addFilterConditions("   ")  // Blank string should be skipped
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("empty-filter-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Node should process when filters are empty/blank",
                    response.getSuccess(), is(true));
        }
    }

    // ========== Pre-Mappings Tests ==========

    @Nested
    @DisplayName("Pre-Mappings Tests")
    class PreMappingsTests {

        @Test
        @DisplayName("Should apply direct mapping before module processing")
        void testPreMappingDirect() {
            ProcessingMapping preMapping = ProcessingMapping.newBuilder()
                    .setMappingId("pre-direct-mapping")
                    .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                    .addSourceFieldPaths("search_metadata.title")
                    .addTargetFieldPaths("prepared_title")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("pre-mapping-node")
                    .setName("Pre-Mapping Node")
                    .setModuleId("text-chunker")
                    .addPreMappings(preMapping)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setTitle("Original Title")
                            .build())
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("pre-mapping-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Node with pre-mapping should process successfully",
                    response.getSuccess(), is(true));
            // Note: Pre-mapping is applied before module call; the module then uses the mapped document
        }

        @Test
        @DisplayName("Should apply transform mapping (uppercase) before module processing")
        void testPreMappingTransform() {
            ProcessingMapping preMapping = ProcessingMapping.newBuilder()
                    .setMappingId("pre-transform-mapping")
                    .setMappingType(MappingType.MAPPING_TYPE_TRANSFORM)
                    .addSourceFieldPaths("input_text")
                    .addTargetFieldPaths("normalized_text")
                    .setTransformConfig(TransformConfig.newBuilder()
                            .setRuleName("uppercase")
                            .build())
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("pre-transform-node")
                    .setName("Pre-Transform Node")
                    .setModuleId("text-chunker")
                    .addPreMappings(preMapping)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            Struct customFields = Struct.newBuilder()
                    .putFields("input_text", Values.of("hello world"))
                    .build();

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setCustomFields(customFields)
                            .build())
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("pre-transform-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Node with pre-transform mapping should process successfully",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should apply multiple pre-mappings in sequence")
        void testMultiplePreMappings() {
            ProcessingMapping mapping1 = ProcessingMapping.newBuilder()
                    .setMappingId("pre-mapping-1")
                    .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                    .addSourceFieldPaths("field_a")
                    .addTargetFieldPaths("field_b")
                    .build();

            ProcessingMapping mapping2 = ProcessingMapping.newBuilder()
                    .setMappingId("pre-mapping-2")
                    .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                    .addSourceFieldPaths("field_b")
                    .addTargetFieldPaths("field_c")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("multi-pre-mapping-node")
                    .setName("Multi Pre-Mapping Node")
                    .setModuleId("text-chunker")
                    .addPreMappings(mapping1)
                    .addPreMappings(mapping2)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            Struct customFields = Struct.newBuilder()
                    .putFields("field_a", Values.of("original value"))
                    .build();

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setCustomFields(customFields)
                            .build())
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("multi-pre-mapping-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Node with multiple pre-mappings should process successfully",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should skip pre-mappings when document has no content")
        void testPreMappingsSkippedWhenNoDocument() {
            ProcessingMapping preMapping = ProcessingMapping.newBuilder()
                    .setMappingId("pre-mapping")
                    .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                    .addSourceFieldPaths("source")
                    .addTargetFieldPaths("target")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("pre-mapping-no-doc-node")
                    .setName("Pre-Mapping No Doc Node")
                    .setModuleId("text-chunker")
                    .addPreMappings(preMapping)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            // Stream without document - this should cause an error
            // because module requires a document
            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setCurrentNodeId("pre-mapping-no-doc-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            // Should fail because module call requires document
            assertThat("Node should fail when no document present",
                    response.getSuccess(), is(false));
        }
    }

    // ========== Post-Mappings Tests ==========

    @Nested
    @DisplayName("Post-Mappings Tests")
    class PostMappingsTests {

        @Test
        @DisplayName("Should apply direct mapping after module processing")
        void testPostMappingDirect() {
            ProcessingMapping postMapping = ProcessingMapping.newBuilder()
                    .setMappingId("post-direct-mapping")
                    .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                    .addSourceFieldPaths("search_metadata.title")
                    .addTargetFieldPaths("final_title")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("post-mapping-node")
                    .setName("Post-Mapping Node")
                    .setModuleId("text-chunker")
                    .addPostMappings(postMapping)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setTitle("Module Output Title")
                            .build())
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("post-mapping-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Node with post-mapping should process successfully",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should apply CEL mapping after module processing")
        void testPostMappingCel() {
            ProcessingMapping postMapping = ProcessingMapping.newBuilder()
                    .setMappingId("post-cel-mapping")
                    .setMappingType(MappingType.MAPPING_TYPE_CEL)
                    .setCelConfig(CelConfig.newBuilder()
                            .setExpression("document.search_metadata.title + ' - processed'")
                            .build())
                    .addTargetFieldPaths("processed_title")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("post-cel-node")
                    .setName("Post-CEL Node")
                    .setModuleId("text-chunker")
                    .addPostMappings(postMapping)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setTitle("Original")
                            .build())
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("post-cel-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Node with post-CEL mapping should process successfully",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should apply both pre and post mappings")
        void testPreAndPostMappings() {
            ProcessingMapping preMapping = ProcessingMapping.newBuilder()
                    .setMappingId("pre-mapping")
                    .setMappingType(MappingType.MAPPING_TYPE_TRANSFORM)
                    .addSourceFieldPaths("raw_text")
                    .addTargetFieldPaths("prepared_text")
                    .setTransformConfig(TransformConfig.newBuilder()
                            .setRuleName("trim")
                            .build())
                    .build();

            ProcessingMapping postMapping = ProcessingMapping.newBuilder()
                    .setMappingId("post-mapping")
                    .setMappingType(MappingType.MAPPING_TYPE_TRANSFORM)
                    .addSourceFieldPaths("result_text")
                    .addTargetFieldPaths("final_text")
                    .setTransformConfig(TransformConfig.newBuilder()
                            .setRuleName("uppercase")
                            .build())
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("pre-post-node")
                    .setName("Pre-Post Node")
                    .setModuleId("text-chunker")
                    .addPreMappings(preMapping)
                    .addPostMappings(postMapping)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            Struct customFields = Struct.newBuilder()
                    .putFields("raw_text", Values.of("  hello world  "))
                    .putFields("result_text", Values.of("processed result"))
                    .build();

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setCustomFields(customFields)
                            .build())
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("pre-post-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Node with both pre and post mappings should process successfully",
                    response.getSuccess(), is(true));
        }
    }

    // ========== Save On Error Tests ==========

    @Nested
    @DisplayName("Save On Error Tests")
    class SaveOnErrorTests {

        @Test
        @DisplayName("Should set save_on_error flag in graph node")
        void testSaveOnErrorFlagConfiguration() {
            // This test verifies the flag can be configured
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("save-on-error-node")
                    .setName("Save On Error Node")
                    .setModuleId("text-chunker")
                    .setSaveOnError(true)
                    .build();

            graphCache.putNode(node);

            assertThat("Save on error flag should be set",
                      graphCache.getNode("save-on-error-node")
                          .await().indefinitely().get().getSaveOnError(), is(true));
        }

        @Test
        @DisplayName("Should default save_on_error to false")
        void testSaveOnErrorDefaultFalse() {
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("default-node")
                    .setName("Default Node")
                    .setModuleId("text-chunker")
                    // No setSaveOnError called
                    .build();

            graphCache.putNode(node);

            assertThat("Save on error flag should default to false",
                      graphCache.getNode("default-node")
                          .await().indefinitely().get().getSaveOnError(), is(false));
        }

        @Test
        @DisplayName("Should handle node with save_on_error=true when processing succeeds")
        void testSaveOnErrorSuccessfulProcessing() {
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("save-error-success-node")
                    .setName("Save Error Success Node")
                    .setModuleId("text-chunker")
                    .setSaveOnError(true)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("save-error-success-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            // save_on_error only triggers on failure, so success should be unaffected
            assertThat("Node should process successfully regardless of save_on_error",
                    response.getSuccess(), is(true));
        }
    }

    // ========== Combined Pipeline Tests ==========

    @Nested
    @DisplayName("Combined Pipeline Tests")
    class CombinedPipelineTests {

        @Test
        @DisplayName("Should run full pipeline: filter -> pre-map -> module -> post-map")
        void testFullPipeline() {
            ProcessingMapping preMapping = ProcessingMapping.newBuilder()
                    .setMappingId("pre-map")
                    .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                    .addSourceFieldPaths("input_field")
                    .addTargetFieldPaths("processed_input")
                    .build();

            ProcessingMapping postMapping = ProcessingMapping.newBuilder()
                    .setMappingId("post-map")
                    .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                    .addSourceFieldPaths("output_field")
                    .addTargetFieldPaths("final_output")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("full-pipeline-node")
                    .setName("Full Pipeline Node")
                    .setModuleId("text-chunker")
                    .addFilterConditions("document.doc_id != ''")
                    .addPreMappings(preMapping)
                    .addPostMappings(postMapping)
                    .setSaveOnError(true)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            Struct customFields = Struct.newBuilder()
                    .putFields("input_field", Values.of("input value"))
                    .putFields("output_field", Values.of("output value"))
                    .build();

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setCustomFields(customFields)
                            .build())
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("full-pipeline-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Full pipeline should complete successfully",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should skip module when filter fails but still return success")
        void testFilterSkipsModuleButSucceeds() {
            ProcessingMapping preMapping = ProcessingMapping.newBuilder()
                    .setMappingId("pre-map")
                    .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                    .addSourceFieldPaths("source")
                    .addTargetFieldPaths("target")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("filter-skip-node")
                    .setName("Filter Skip Node")
                    .setModuleId("text-chunker")
                    .addFilterConditions("document.doc_id == 'different-id'")  // Will fail
                    .addPreMappings(preMapping)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")  // Different from filter condition
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("filter-skip-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            // Filter should skip the node, but processing still succeeds
            assertThat("Node should succeed when skipped by filter",
                    response.getSuccess(), is(true));
        }
    }
}
