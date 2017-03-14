import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by xingruibo on 2017/3/14.
 */
public class SubmitTopology {

    @Test
    public void submitJar() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        conf.put(Config.NIMBUS_SEEDS, "localhost");
        conf.setDebug(true);
        Map storm_conf = Utils.readStormConfig();
        storm_conf.put("nimbus.seeds", Arrays.asList("localhost"));
        Nimbus.Client client = NimbusClient.getConfiguredClient(storm_conf)
                .getClient();
        String inputJar = "D:\\MyProject\\StormDemo\\target\\StormDemo-1.0-SNAPSHOT.jar";
        NimbusClient nimbus = null;
        try {
            nimbus = new NimbusClient(storm_conf, "localhost",
                    6627);
        } catch (TTransportException e) {
            e.printStackTrace();
        }
        // upload topology jar to Cluster using StormSubmitter
        String uploadedJarLocation = StormSubmitter.submitJar(storm_conf,
                inputJar);
        try {
            String jsonConf = JSONValue.toJSONString(storm_conf);
            nimbus.getClient().submitTopology("word-count",
                    uploadedJarLocation, jsonConf, builder.createTopology());
        } catch (AlreadyAliveException ae) {
            ae.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        Thread.sleep(60000);
    }
}
