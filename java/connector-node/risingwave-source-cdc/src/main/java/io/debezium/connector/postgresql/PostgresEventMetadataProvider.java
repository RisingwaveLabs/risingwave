/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.time.Conversions;
import io.debezium.util.Collect;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;

class PostgresEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(
            DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        if (sourceInfo.schema().field(SourceInfo.TIMESTAMP_USEC_KEY) != null) {
            final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_USEC_KEY);
            return timestamp == null ? null : Conversions.toInstantFromMicros(timestamp);
        }
        final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(
            DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        final Long xmin = sourceInfo.getInt64(SourceInfo.XMIN_KEY);
        final Long lsn = sourceInfo.getInt64(SourceInfo.LSN_KEY);
        if (lsn == null) {
            return null;
        }

        Map<String, String> r = Collect.hashMapOf(SourceInfo.LSN_KEY, Long.toString(lsn));
        if (xmin != null) {
            r.put(SourceInfo.XMIN_KEY, Long.toString(xmin));
        }
        return r;
    }

    @Override
    public String getTransactionId(
            DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        Long txId = sourceInfo.getInt64(SourceInfo.TXID_KEY);
        if (txId == null) {
            return null;
        }
        return Long.toString(txId);
    }
}
