package storm.kafka;

import com.google.common.collect.Maps;
import kafka.api.ConsumerMetadataRequest;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.network.BlockingChannel;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaBackedPartitionStateManager implements PartitionStateManager {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaBackedPartitionStateManager.class);

    private SpoutConfig _spoutConfig;
    private Partition _partition;

    private String _consumerGroupId;
    private String _consumerClientId;
    private int _stateOpTimeout;
    private int _stateOpMaxRetry;

    private int _correlationId = 0;
    private BlockingChannel _offsetManager;

    public KafkaBackedPartitionStateManager(Map stormConf, SpoutConfig spoutConfig, Partition partition) {
        this._spoutConfig = spoutConfig;
        this._partition = partition;
        this._consumerGroupId = _spoutConfig.id;
        this._consumerClientId = _spoutConfig.clientId;
        this._stateOpTimeout = _spoutConfig.stateOpTimeout;
        this._stateOpMaxRetry = _spoutConfig.stateOpMaxRetry;
    }

    // as there is a manager per topic per partition and the stateUpdateIntervalMs should not be too small
    // feels ok to  place a sync here
    private synchronized BlockingChannel locateOffsetManager() {
        if (_offsetManager == null) {
            BlockingChannel channel = new BlockingChannel(_partition.host.host, _partition.host.port,
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    _stateOpTimeout /* read timeout in millis */);
            channel.connect();
            channel.send(new ConsumerMetadataRequest(_consumerGroupId, ConsumerMetadataRequest.CurrentVersion(),
                    _correlationId++, _consumerClientId));
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
                            _stateOpTimeout /* read timeout in millis */);
                    channel.connect();
                }
                _offsetManager = channel;

            } else {
                throw new RuntimeException("Kafka metadata fetch error: " + metadataResponse.errorCode());
            }
        }
        return _offsetManager;
    }

    private Map<Object, Object> attemptToGetState() {
        Map<Object, Object> state = null;
        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        TopicAndPartition thisTopicPartition = new TopicAndPartition(_spoutConfig.topic, _partition.partition);
        partitions.add(thisTopicPartition);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                _consumerGroupId,
                partitions,
                (short) 1, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                _correlationId++,
                _consumerClientId);

        BlockingChannel offsetManager = locateOffsetManager();
        offsetManager.send(fetchRequest.underlying());
        OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(offsetManager.receive().buffer());
        OffsetMetadataAndError result = fetchResponse.offsets().get(thisTopicPartition);
        if (result.error() == ErrorMapping.NoError()) {
            Object retrievedMetadata = result.metadata();
            if (retrievedMetadata != null) {
                state = (Map<Object, Object>) JSONValue.parse(retrievedMetadata.toString());
            } else {
                // let it return null, this maybe the first time it is called before the state is persisted
            }

        } else {
            // may attempt to implement retry later
            throw new RuntimeException("Kafka offset fetch error: " + result.error());
        }
        return state;
    }

    @Override
    public Map<Object, Object> getState() {
        int attemptCount = 0;
        while (true) {
            try {
                return attemptToGetState();

            } catch(RuntimeException re) {
                if (++attemptCount > _stateOpMaxRetry) {
                    throw re;
                } else {
                    LOG.warn("Attempt " + attemptCount + " out of " + _stateOpMaxRetry
                            + ". Failed to fetch state for partition " + _partition.partition
                            + " of topic " + _spoutConfig.topic + " due to Kafka offset fetch error: " + re.getMessage());
                }
            }
        }
    }

    private void attemptToWriteState(Map<Object, Object> data) {
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
                (short) 1); // version 1 and above commit to Kafka, version 0 commits to ZooKeeper

        BlockingChannel offsetManager = locateOffsetManager();
        offsetManager.send(commitRequest.underlying());
        OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(offsetManager.receive().buffer());
        if (commitResponse.hasError()) {
            // note: here we should have only 1 error for the partition in request
            for (Object partitionErrorCode : commitResponse.errors().values()) {
                if (partitionErrorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
                    throw new RuntimeException("Data is too big. The state object is " + stateData);
                } else {
                    // may attempt to implement retry later
                    throw new RuntimeException("Kafka offset commit error: " + partitionErrorCode);
                }
            }
        }
    }

    @Override
    public void writeState(Map<Object, Object> data) {
        int attemptCount = 0;
        while (true) {
            try {
                attemptToWriteState(data);
                return;

            } catch(RuntimeException re) {
                if (++attemptCount > _stateOpMaxRetry) {
                    throw re;
                } else {
                    LOG.warn("Attempt " + attemptCount + " out of " + _stateOpMaxRetry
                            + ". Failed to save state for partition " + _partition.partition
                            + " of topic " + _spoutConfig.topic + " due to Kafka offset commit error: " + re.getMessage());
                }
            }
        }
    }
}
