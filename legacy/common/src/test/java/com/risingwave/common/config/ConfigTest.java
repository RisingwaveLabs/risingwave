package com.risingwave.common.config;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** ConfigTest includes some test on configuration. */
public class ConfigTest {

  @Test
  public void testLeaderServerConfiguration() {
    var lines =
        ImmutableList.of(
            "risingwave.leader.clustermode=Distributed",
            "risingwave.leader.computenodes=127.0.0.1:5688");
    var config = loadConfigFromLines(lines, LeaderServerConfigurations.class);

    var clusterMode = config.get(LeaderServerConfigurations.CLUSTER_MODE);
    Assertions.assertEquals(clusterMode, LeaderServerConfigurations.ClusterMode.Distributed);
    var computeNodes = config.get(LeaderServerConfigurations.COMPUTE_NODES);
    Assertions.assertLinesMatch(computeNodes, List.of("127.0.0.1:5688"));
  }

  private static Configuration loadConfigFromLines(List<String> lines, Class<?>... classes) {
    String configBlock = Joiner.on("\n").join(lines);
    return Configuration.load(new ByteArrayInputStream(configBlock.getBytes()), classes);
  }

  @Test
  public void testVariableConfigurationClasses() {
    var lines =
        ImmutableList.of(
            "risingwave.pgserver.port=4567",
            "risingwave.computenode.port=5688",
            "risingwave.leader.clustermode=Distributed",
            "risingwave.leader.computenodes=127.0.0.1:5688",
            "risingwave.catalog.mode=Local",
            "risingwave.meta.node=127.0.0.1:9527");
    Configuration config =
        loadConfigFromLines(
            lines, FrontendServerConfigurations.class, LeaderServerConfigurations.class);

    var pgwirePort = config.get(FrontendServerConfigurations.PG_WIRE_PORT);
    Assertions.assertEquals(pgwirePort, 4567);
    var pgwireIp = config.get(FrontendServerConfigurations.PG_WIRE_IP);
    Assertions.assertEquals(pgwireIp, "0.0.0.0");
    var clusterMode = config.get(LeaderServerConfigurations.CLUSTER_MODE);
    Assertions.assertEquals(clusterMode, LeaderServerConfigurations.ClusterMode.Distributed);
    var catalogMode = config.get(FrontendServerConfigurations.CATALOG_MODE);
    Assertions.assertEquals(catalogMode, FrontendServerConfigurations.CatalogMode.Local);
    var metaAddress = config.get(FrontendServerConfigurations.META_SERVICE_ADDRESS);
    Assertions.assertEquals(metaAddress, "127.0.0.1:9527");
  }
}
