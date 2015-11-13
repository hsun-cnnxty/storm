package storm.kafka;

import java.io.Closeable;
import java.util.Map;

/**
 * Abstraction of a partition state storage.
 * <p>
 * The partition state usually is kept in Json format in the store and in Map format in runtime memory. An example
 * is shown below:
 * </p>
 *
 * <p>
 * <strong>Json</strong>:
 * <code>
 *  {
 *      "broker": {
 *          "host": "kafka.sample.net",
 *          "port": 9092
 *      },
 *      "offset": 4285,
 *      "partition": 1,
 *      "topic": "testTopic",
 *      "topology": {
 *          "id": "fce905ff-25e0 -409e-bc3a-d855f 787d13b",
 *          "name": "Test Topology"
 *      }
 *  }
 * </code>
 * </p>
 *
 * <p>
 * <strong>Memory</strong>:
 * <code>
 *  Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
 *        .put("topology", ImmutableMap.of(
 *                "id", "fce905ff-25e0 -409e-bc3a-d855f 787d13b",
 *                "name", "Test Topology"))
 *        .put("offset", 4285)
 *        .put("partition", 1)
 *        .put("broker", ImmutableMap.of(
 *                "host", "kafka.sample.net",
 *                "port", 9092))
 *        .put("topic", "testTopic").build();
 *
 * </code>
 * </p>
 *
 * <p>
 * User can create their own custom state store by implementing this interface and register it with
 * {@link SpoutConfig#stateStore}. The implementation class must also provide a public constructor
 * that takes two arguments:
 * </p>
 * 
 * <code>
 *   public CustomStateStor(Map stormConf and SpoutConfig spoutConfig)
 * </code>
 *
 * <p>
 * See {@see KafkaStateStore} class as an example.
 * </p>
 */
public interface StateStore extends Closeable {

    Map<Object, Object> readState(Partition p);

    void writeState(Partition p, Map<Object, Object> state);
}