package com.risingwave.execution.result.rpc.string;

import com.google.common.base.Charsets;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.result.rpc.PgValueReaderBase;
import com.risingwave.execution.result.rpc.primitive.BooleanBufferReader;
import com.risingwave.execution.result.rpc.primitive.LongBufferReader;
import com.risingwave.pgwire.types.PgValue;
import java.io.InputStream;
import java.util.function.Function;
import javax.annotation.Nullable;

public class StringValueReader extends PgValueReaderBase {
  private final Function<String, PgValue> transformer;
  private final LongBufferReader offsetBuffer;
  private final InputStream bytesStream;
  private long prevOffset = -1;

  public StringValueReader(
      Function<String, PgValue> transformer,
      LongBufferReader offsetBuffer,
      InputStream bytesStream,
      @Nullable BooleanBufferReader nullBitmapReader) {
    super(nullBitmapReader);
    this.transformer = transformer;
    this.offsetBuffer = offsetBuffer;
    this.bytesStream = bytesStream;
  }

  private long readLength() throws Exception {
    long offset = this.offsetBuffer.next();

    if (this.prevOffset < 0) {
      this.prevOffset = offset;
      offset = this.offsetBuffer.next();
    }

    long result = offset - this.prevOffset;
    this.prevOffset = offset;
    return result;
  }

  public String readNext() {
    try {
      int length = (int) this.readLength();
      return new String(this.bytesStream.readNBytes(length), 0, length, Charsets.UTF_8);
    } catch (Exception e) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, e);
    }
  }

  @Override
  protected PgValue nextValue() {
    return transformer.apply(readNext());
  }

  public static StringValueReader createValueReader(
      Function<String, PgValue> transformer,
      LongBufferReader offsetBuffer,
      InputStream bytesStream,
      @Nullable BooleanBufferReader nullBitmap) {
    return new StringValueReader(transformer, offsetBuffer, bytesStream, nullBitmap);
  }
}
