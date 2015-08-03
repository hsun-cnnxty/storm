package storm.kafka;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import kafka.api.ConsumerMetadataRequest;
import kafka.cluster.*;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.network.BlockingChannel;
import org.json.simple.JSONValue;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by 155715 on 8/3/15.
 */
public class KafkaBackedPartitionStateManager implements PartitionStateManager {

    private static final Splitter.MapSplitter STATE_SPLITTER = Splitter.on('|').withKeyValueSeparator("=");
    private static final Joiner.MapJoiner STATE_JOINER = Joiner.on('|').withKeyValueSeparator("=");

    private Map _stormConf;
    private SpoutConfig _spoutConfig;
    private Partition _partition;
    private String _topologyInstanceId;

    private String _consumerGroupId = _spoutConfig.id;
    private String _consumerClientId = _spoutConfig.clientId;
    private int _correlationId = 0;

    public KafkaBackedPartitionStateManager(Map stormConf, SpoutConfig spoutConfig, String topologyInstanceId, Partition partition) {
        this._stormConf = stormConf;
        this._spoutConfig = spoutConfig;
        this._partition = partition;
        this._topologyInstanceId = topologyInstanceId;
    }

    private BlockingChannel locateOffsetManager() {
        BlockingChannel channel = new BlockingChannel(_partition.host.host, _partition.host.port,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                5000 /* read timeout in millis */);
        channel.connect();
        channel.send(new ConsumerMetadataRequest(_consumerGroupId, ConsumerMetadataRequest.CurrentVersion(),
                _correlationId, _consumerClientId));
        ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

        if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
            kafka.cluster.Broker offsetManager = metadataResponse.coordinator();
            if (!offsetManager.host().equals(_partition.host.host)
                    || !(offsetManager.port() == _partition.host.port)) {
                // if the coordinator is different, from the above channel's host then reconnect
                channel.disconnect();
                channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                        BlockingChannel.UseDefaultBufferSize(),
                        BlockingChannel.UseDefaultBufferSize(),
                        5000 /* read timeout in millis */);
                channel.connect();
            }

        } else {
            // may attempt to implement retry later
            throw new RuntimeException("Failed to locate offset manager for partition " + _partition.partition
                    + " of topic " + _spoutConfig.topic + " due to Kafka metadata fetch error. The error code is "
                    + metadataResponse.errorCode());
        }
        return channel;
    }

    @Override
    public Map<Object, Object> getState() {

        Map<Object, Object> state = null;
        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        TopicAndPartition thisTopicPartition = new TopicAndPartition(_spoutConfig.topic, _partition.partition);
        partitions.add(thisTopicPartition);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                _consumerGroupId,
                partitions,
                (short) 1 /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                _correlationId,
                _consumerClientId);

        BlockingChannel offsetManager = null;
        try {
            offsetManager = locateOffsetManager();
            offsetManager.send(fetchRequest.underlying());
            OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(offsetManager.receive().buffer());
            OffsetMetadataAndError result = fetchResponse.offsets().get(thisTopicPartition);
            if (result.error() == ErrorMapping.NoError()) {
                long retrievedOffset = result.offset();
                Object retrievedMetadata = result.metadata();
                if (retrievedMetadata != null) {
                    state = (Map<Object, Object>) JSONValue.parse(retrievedMetadata.toString());
                } else {
                    // let it return null, this maybe the first time it is called before the state is persisted
                }

            } else {
                // may attempt to implement retry later
                throw new RuntimeException("Failed to fetch state for partition " + _partition.partition
                        + " of topic " + _spoutConfig.topic + " due to Kafka offset fetch error. The error code is "
                        + result.error());
            }

        } finally {
            if (offsetManager != null) {
                offsetManager.disconnect();
            }
        }
        return state;
    }

    @Override
    public void writeState(Map<Object, Object> data) {
        long now = System.currentTimeMillis();
        Map<TopicAndPartition, OffsetAndMetadata> offsets = Maps.newLinkedHashMap();
        TopicAndPartition thisTopicPartition = new TopicAndPartition(_spoutConfig.topic, _partition.partition);
        Long offsetOfPartition = (Long)data.get("offset");
        String stateData = JSONValue.toJSONString(data);
        offsets.put(thisTopicPartition, new OffsetAndMetadata(
                offsetOfPartition,
                stateData,
                now));
        OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                _consumerGroupId,
                offsets,
                _correlationId,
                _consumerClientId,
                (short) 1 /* version */); // version 1 and above commit to Kafka, version 0 commits to ZooKeeper

        BlockingChannel offsetManager = null;
        try {
            offsetManager = locateOffsetManager();
            offsetManager.send(commitRequest.underlying());
            OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(offsetManager.receive().buffer());
            if (commitResponse.hasError()) {
                // note: here we should have only 1 error for the partition in request
                for (Object partitionErrorCode: commitResponse.errors().values()) {
                    if (partitionErrorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
                        throw new RuntimeException("Failed to save state for partition " + _partition.partition
                                + " of topic " + _spoutConfig.topic + " b/c the data is too big. The state object is " + stateData);
                    } else {
                        // may attempt to implement retry later
                        throw new RuntimeException("Failed to save state for partition " + _partition.partition
                                + " of topic " + _spoutConfig.topic + " due to Kafka offset commit error. The error code is " + partitionErrorCode);
                    }
                }
            }

        } finally {
            if (offsetManager != null) {
                offsetManager.disconnect();
            }
        }
    }
}
