# Module Integration

## Module Contract

Modules implement a simple gRPC interface:

```protobuf
service PipeStepProcessorService {
  // Process a document
  rpc ProcessData(ProcessDataRequest) returns (ProcessDataResponse);
  
  // Return module metadata
  rpc GetServiceRegistration(GetServiceRegistrationRequest) 
      returns (GetServiceRegistrationResponse);
}
```

## Module Characteristics

| Property | Description |
|----------|-------------|
| **Stateless** | No memory between calls |
| **Unaware** | Doesn't know about graph, routing, other nodes |
| **Pure function** | `PipeDoc in → PipeDoc out` |
| **Language agnostic** | Any language that speaks gRPC |
| **Independently scalable** | 0 → N pods based on demand |

## What Modules DON'T Do

- ❌ Routing decisions
- ❌ Field mapping
- ❌ Filtering
- ❌ Graph awareness
- ❌ State management
- ❌ Kafka interaction
- ❌ S3 interaction (unless specific to their function)

## What Modules DO

- ✅ Transform PipeDoc content
- ✅ Add/modify metadata
- ✅ Parse documents (PARSER)
- ✅ Chunk text (CHUNKER)
- ✅ Generate embeddings (EMBEDDER)
- ✅ Output to external systems (SINK)
- ✅ Report capabilities for UI adaptation

## ProcessDataRequest

```protobuf
message ProcessDataRequest {
  // The document to process
  PipeDoc document = 1;
  
  // Module-specific configuration
  ProcessConfiguration config = 2;
  
  // Context from engine (optional use)
  ServiceMetadata metadata = 3;
  
  // Post-mappings for engine to apply after
  repeated ProcessingMapping post_mappings = 5;
  
  // Test mode flag
  bool is_test = 6;
}
```

## ProcessDataResponse

```protobuf
message ProcessDataResponse {
  // Did processing succeed?
  bool success = 1;
  
  // The transformed document
  PipeDoc output_doc = 2;
  
  // Processing logs for debugging
  repeated string processor_logs = 3;
  
  // Error details if success=false
  optional string error_message = 4;
}
```

## Module Discovery

Modules register with the platform registration service:

```protobuf
message RegisterRequest {
  string name = 1;                    // "tika-parser"
  ServiceType type = 2;               // SERVICE_TYPE_MODULE
  Connectivity connectivity = 3;       // host:port
  string version = 4;                  // "1.2.3"
  map<string, string> metadata = 5;
  repeated string tags = 6;           // ["parser", "pdf"]
  repeated string capabilities = 7;   // ["PARSER"]
}
```

Engine discovers modules via Consul:

```java
public class ModuleDiscovery {
    private final ConsulClient consul;
    
    public ServiceInstance getModule(String moduleName) {
        List<ServiceInstance> instances = consul.getHealthyInstances(moduleName);
        if (instances.isEmpty()) {
            throw new ModuleUnavailableException(moduleName);
        }
        // Load balance
        return instances.get(random.nextInt(instances.size()));
    }
    
    public boolean isModuleHealthy(String moduleName) {
        return !consul.getHealthyInstances(moduleName).isEmpty();
    }
}
```

## Module Capabilities

Modules declare what they can do:

```protobuf
message ModuleCapabilities {
  repeated ModuleCapability capabilities = 1;
  repeated WritableSection writable_sections = 2;
  bool can_modify_blobs = 3;
  bool can_create_documents = 4;
}

enum ModuleCapability {
  MODULE_CAPABILITY_UNSPECIFIED = 0;
  MODULE_CAPABILITY_PARSER = 1;       // Raw → parsed_metadata
  MODULE_CAPABILITY_PROCESSOR = 10;   // General transformation
  MODULE_CAPABILITY_CHUNKER = 11;     // Splits into chunks
  MODULE_CAPABILITY_EMBEDDER = 12;    // Generates vectors
  MODULE_CAPABILITY_ENRICHER = 13;    // Adds metadata
  MODULE_CAPABILITY_FILTER = 14;      // Pass/drop (no modification)
  MODULE_CAPABILITY_SINK = 20;        // Outputs externally
}
```

**Usage:**

| Consumer | Uses Capabilities For |
|----------|----------------------|
| **Frontend** | Show appropriate config UI |
| **Engine** | Validate output (PARSER only touches `parsed_metadata`) |
| **OOTB Mappings** | Suggest default mappings |

## Engine Calling Module

```java
public class ModuleCaller {
    private final ModuleDiscovery discovery;
    private final ChannelPool channelPool;
    
    public ProcessDataResponse call(String moduleName, ProcessDataRequest request) {
        // Discover healthy instance
        ServiceInstance instance = discovery.getModule(moduleName);
        
        // Get or create channel
        ManagedChannel channel = channelPool.getChannel(
            instance.getHost(), 
            instance.getPort()
        );
        
        // Create stub with timeout
        PipeStepProcessorServiceBlockingStub stub = 
            PipeStepProcessorServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(30, TimeUnit.SECONDS);
        
        // Call
        return stub.processData(request);
    }
}
```

## Error Handling

| Scenario | Engine Action |
|----------|---------------|
| `success=false` | Log error in history, continue to next nodes |
| Module unreachable | Route to DLQ |
| gRPC timeout | Retry with backoff, then DLQ |
| Module returns invalid doc | Validate against capabilities, reject if invalid |

## Module Implementation Example (Java)

```java
public class MyChunkerModule extends PipeStepProcessorServiceImplBase {
    
    @Override
    public void processData(ProcessDataRequest request, 
                           StreamObserver<ProcessDataResponse> observer) {
        try {
            PipeDoc input = request.getDocument();
            
            // Get text to chunk
            String text = input.getSearchMetadata().getBody();
            
            // Chunk it
            List<SemanticChunk> chunks = chunker.chunk(text);
            
            // Build output
            PipeDoc output = input.toBuilder()
                .setSearchMetadata(input.getSearchMetadata().toBuilder()
                    .setSemanticResults(SemanticResults.newBuilder()
                        .addAllSemanticChunks(chunks)
                        .build())
                    .build())
                .build();
            
            observer.onNext(ProcessDataResponse.newBuilder()
                .setSuccess(true)
                .setOutputDoc(output)
                .build());
                
        } catch (Exception e) {
            observer.onNext(ProcessDataResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(e.getMessage())
                .build());
        }
        observer.onCompleted();
    }
    
    @Override
    public void getServiceRegistration(GetServiceRegistrationRequest request,
                                       StreamObserver<GetServiceRegistrationResponse> observer) {
        observer.onNext(GetServiceRegistrationResponse.newBuilder()
            .setModuleName("my-chunker")
            .setVersion("1.0.0")
            .setDisplayName("My Chunker Module")
            .setCapabilities(ModuleCapabilities.newBuilder()
                .addCapabilities(MODULE_CAPABILITY_CHUNKER)
                .addWritableSections(WRITABLE_SECTION_SEMANTIC_RESULTS)
                .build())
            .setHealthCheckPassed(true)
            .build());
        observer.onCompleted();
    }
}
```
