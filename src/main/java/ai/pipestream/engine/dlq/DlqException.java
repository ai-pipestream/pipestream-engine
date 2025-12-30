package ai.pipestream.engine.dlq;

/**
 * Base exception for DLQ-related errors.
 * <p>
 * Provides a hierarchy of exception types for different DLQ failure scenarios,
 * allowing for more precise error handling and classification.
 */
public class DlqException extends RuntimeException {

    public DlqException(String message) {
        super(message);
    }

    public DlqException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Thrown when a message has been quarantined due to exceeding max reprocess attempts.
     * <p>
     * Quarantined messages require manual intervention and should not be automatically retried.
     */
    public static class QuarantinedException extends DlqException {
        private final int reprocessCount;
        private final int maxReprocessCount;
        private final String streamId;

        public QuarantinedException(String streamId, int reprocessCount, int maxReprocessCount) {
            super(String.format("Message quarantined: stream %s exceeded max reprocess count (%d/%d)",
                    streamId, reprocessCount, maxReprocessCount));
            this.streamId = streamId;
            this.reprocessCount = reprocessCount;
            this.maxReprocessCount = maxReprocessCount;
        }

        public QuarantinedException(String streamId, int reprocessCount, int maxReprocessCount, Throwable cause) {
            super(String.format("Message quarantined: stream %s exceeded max reprocess count (%d/%d)",
                    streamId, reprocessCount, maxReprocessCount), cause);
            this.streamId = streamId;
            this.reprocessCount = reprocessCount;
            this.maxReprocessCount = maxReprocessCount;
        }

        public int getReprocessCount() {
            return reprocessCount;
        }

        public int getMaxReprocessCount() {
            return maxReprocessCount;
        }

        public String getStreamId() {
            return streamId;
        }
    }

    /**
     * Thrown when attempting to reprocess an already quarantined message.
     */
    public static class AlreadyQuarantinedException extends DlqException {
        private final String streamId;

        public AlreadyQuarantinedException(String streamId) {
            super(String.format("Cannot reprocess already quarantined message: stream %s", streamId));
            this.streamId = streamId;
        }

        public String getStreamId() {
            return streamId;
        }
    }

    /**
     * Thrown when DLQ publishing fails.
     */
    public static class PublishException extends DlqException {
        private final String topic;
        private final String streamId;

        public PublishException(String streamId, String topic, Throwable cause) {
            super(String.format("Failed to publish to DLQ topic '%s' for stream %s", topic, streamId), cause);
            this.topic = topic;
            this.streamId = streamId;
        }

        public String getTopic() {
            return topic;
        }

        public String getStreamId() {
            return streamId;
        }
    }

    /**
     * Thrown when DLQ is disabled but publish was attempted.
     */
    public static class DlqDisabledException extends DlqException {
        private final String nodeId;

        public DlqDisabledException(String nodeId) {
            super(String.format("DLQ is disabled for node %s", nodeId));
            this.nodeId = nodeId;
        }

        public String getNodeId() {
            return nodeId;
        }
    }
}
