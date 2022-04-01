package com.risingwave.planner.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/** Customized RisingWaveRelMetadataProvider */
public class RisingWaveRelMetadataProvider {
  private static final RelMetadataProvider INSTANCE =
      ChainedRelMetadataProvider.of(
          ImmutableList.of(DefaultRelMetadataProvider.INSTANCE, RisingWaveRelMdPrimaryKey.SOURCE));

  public static RelMetadataProvider getMetadataProvider() {
    var provider = JaninoRelMetadataProvider.of(INSTANCE);
    RelMetadataQuery.THREAD_PROVIDERS.set(provider);
    //    Consumer<Holder<Boolean>> c = h -> h.set(false);
    //    Hook.REL_BUILDER_SIMPLIFY.addThread(c);
    return provider;
  }
}
