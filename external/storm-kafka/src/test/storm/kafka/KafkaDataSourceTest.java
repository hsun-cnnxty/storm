package storm.kafka;

import backtype.storm.Config;
import kafka.admin.AdminUtils;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created by 155715 on 8/21/15.
 */
public class KafkaDataSourceTest {

    private KafkaTestBroker testBroker;
    private SimpleConsumer simpleConsumer;
    private KafkaDataStore dataStore;

    @Before
    public void setUp() throws Exception {

//        MockitoAnnotations.initMocks(this);

        String testTopic = "testTopic";

        TestingServer server = new TestingServer();
        Thread.sleep(1000l);

//        new KafkaTestBroker(server, 2);
//        new KafkaTestBroker(server, 1);
        testBroker = new KafkaTestBroker(server, 0);
        String connectionString = server.getConnectString();

//        ZkClient zkClient = new ZkClient(connectionString);
//        AdminUtils.createTopic(zkClient, testTopic, 1, 1, new Properties());
//        System.out.println("Created topic: " + testTopic);

//        AdminUtils.createTopic(zkClient, "__consumer_offsets", 10, 3, new Properties());
//        System.out.println("Created topic: __consumer_offsets");

//        zkClient.close();
//        Thread.sleep(1000l);



        Properties props = new Properties();
        props.put("metadata.broker.list", testBroker.getBrokerConnectionString());
        Producer p = new Producer(new ProducerConfig(props));
        KeyedMessage msg = new KeyedMessage(testTopic, "test message".getBytes());
        p.send(msg);
        System.err.println("Sent msg to topic: " + testTopic);


        ZkHosts hosts = new ZkHosts(connectionString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, testTopic, "/", "testConsumerGroup");

        Map stormConf = new HashMap();

        Broker broker = new Broker("localhost", testBroker.getPort());
        Partition testPartition = new Partition(broker, 0);

//        simpleConsumer = new SimpleConsumer("localhost", testBroker.getPort(), 60000, 1024, "testConsumerGroup");

        dataStore = new KafkaDataStore(stormConf, spoutConfig, testPartition);
    }

    @After
    public void shutdown() throws Exception {
//        testBroker.shutdown();
    }

    @Test
    public void testStoreReadWrite() {

        Long offset = 100L;
        String testData = "abcdefg";

        dataStore.write(offset, testData);

        System.err.println("Wrote data");

        String readBack = dataStore.read();

        assertEquals(testData, readBack);
    }
}
