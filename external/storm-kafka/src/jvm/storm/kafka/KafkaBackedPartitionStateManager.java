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

    private KafkaDataStore _dataStore;

    public KafkaBackedPartitionStateManager(Map stormConf, SpoutConfig spoutConfig, Partition partition, KafkaDataStore dataStore) {
        this._dataStore = dataStore;
    }

    @Override
    public Map<Object, Object> getState() {
        return  (Map<Object, Object>) JSONValue.parse(_dataStore.read());
    }

    @Override
    public void writeState(Map<Object, Object> data) {
        Long offsetOfPartition = (Long)data.get("offset");
        String stateData = JSONValue.toJSONString(data);
        _dataStore.write(offsetOfPartition, stateData);
    }
}