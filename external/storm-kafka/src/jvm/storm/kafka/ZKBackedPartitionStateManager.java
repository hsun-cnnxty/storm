package storm.kafka;

import java.util.Map;

public class ZKBackedPartitionStateManager implements PartitionStateManager  {

    private SpoutConfig _spoutConfig;
    private Partition _partition;
    private ZkState _state;

    public ZKBackedPartitionStateManager(SpoutConfig spoutConfig, ZkState state, Partition partition) {
        this._spoutConfig = spoutConfig;
        this._partition = partition;
        this._state = state;
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition.getId();
    }

    @Override
    public Map<Object, Object> getState() {
        // LOG.info("Read partition information from: " + path +  "  --> " + json );
        return _state.readJSON(committedPath());
    }

    @Override
    public void writeState(Map<Object, Object> data) {
        _state.writeJSON(committedPath(), data);
    }
}
