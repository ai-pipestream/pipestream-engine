package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.PipelineGraph;

/**
 * CDI event fired when a new pipeline graph is loaded into the cache.
 * <p>
 * This event enables other services (like CelEvaluatorService) to react to graph changes,
 * for example by pre-compiling CEL expressions to avoid latency spikes on the first document.
 * <p>
 * Observers can process this event asynchronously using {@code @ObservesAsync} to avoid
 * blocking the graph loading operation.
 */
public class GraphLoadedEvent {

    private final PipelineGraph graph;
    private final long previousVersion;
    private final long newVersion;

    /**
     * Creates a new GraphLoadedEvent.
     *
     * @param graph The pipeline graph that was loaded
     * @param previousVersion The version of the graph before this load (0 if first load)
     * @param newVersion The version of the newly loaded graph
     */
    public GraphLoadedEvent(PipelineGraph graph, long previousVersion, long newVersion) {
        this.graph = graph;
        this.previousVersion = previousVersion;
        this.newVersion = newVersion;
    }

    /**
     * Returns the pipeline graph that was loaded.
     *
     * @return The loaded PipelineGraph
     */
    public PipelineGraph getGraph() {
        return graph;
    }

    /**
     * Returns the version of the graph before this load.
     *
     * @return The previous version number (0 if this is the first load)
     */
    public long getPreviousVersion() {
        return previousVersion;
    }

    /**
     * Returns the version of the newly loaded graph.
     *
     * @return The new version number
     */
    public long getNewVersion() {
        return newVersion;
    }

    /**
     * Returns whether this is the first graph load (no previous version).
     *
     * @return true if previousVersion is 0
     */
    public boolean isFirstLoad() {
        return previousVersion == 0;
    }

    @Override
    public String toString() {
        return String.format("GraphLoadedEvent{graphId=%s, previousVersion=%d, newVersion=%d}",
                graph.getGraphId(), previousVersion, newVersion);
    }
}
