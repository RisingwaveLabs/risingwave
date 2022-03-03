package com.risingwave.scheduler;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.SpawnProtocol;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.node.LocalWorkerNodeManager;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.TestComputeClientManager;
import com.risingwave.scheduler.task.RemoteTaskManager;
import com.risingwave.scheduler.task.TaskManager;
import java.io.IOException;
import java.io.InputStream;
import javax.inject.Singleton;

/** Encapsulates the required components to test scheduler. */
public class TestSchedulerModule extends AbstractModule {
  protected void configure() {
    bind(ComputeClientManager.class).to(TestComputeClientManager.class).in(Singleton.class);
    bind(TaskManager.class).to(RemoteTaskManager.class).in(Singleton.class);
    bind(QueryManager.class).to(RemoteQueryManager.class).in(Singleton.class);
    bind(WorkerNodeManager.class).to(LocalWorkerNodeManager.class).in(Singleton.class);
  }

  @Singleton
  @Provides
  static Configuration getConfiguration() {
    try (InputStream input =
        TestSchedulerModule.class.getClassLoader().getResourceAsStream("config.properties")) {
      return Configuration.load(input, LeaderServerConfigurations.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load leader config.", e);
    }
  }

  @Singleton
  @Provides
  static ActorSystem<SpawnProtocol.Command> getActorSystem() {
    return ActorSystem.create(SpawnProtocol.create(), "TestRisingWaveRoot");
  }
}
