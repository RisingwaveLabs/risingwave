package com.risingwave.pgwire.types;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/** Postgresql values implementation. */
public class Values {
  public static PgValue createBoolean(boolean v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        byte b = (byte) (v ? 1 : 0);
        return new byte[] {b};
      }

      @Override
      public String encodeInText() {
        return v ? "t" : "f";
      }
    };
  }

  public static PgValue createInt(int v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(v).array();
      }

      @Override
      public String encodeInText() {
        return String.valueOf(v);
      }
    };
  }

  public static PgValue createBigInt(long v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(v).array();
      }

      @Override
      public String encodeInText() {
        return String.valueOf(v);
      }
    };
  }

  public static PgValue createSmallInt(short v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        return ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort(v).array();
      }

      @Override
      public String encodeInText() {
        return String.valueOf(v);
      }
    };
  }

  public static PgValue createFloat(float v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(v).array();
      }

      @Override
      public String encodeInText() {
        // remove trailing '.0'
        return String.valueOf(v).replaceAll("\\.0$", "");
      }
    };
  }

  public static PgValue createDouble(double v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(v).array();
      }

      @Override
      public String encodeInText() {
        // remove trailing '.0'
        return String.valueOf(v).replaceAll("\\.0$", "");
      }
    };
  }

  public static PgValue createString(String v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        return v.getBytes();
      }

      @Override
      public String encodeInText() {
        return v;
      }
    };
  }

  public static PgValue createDecimal(String v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        return v.getBytes();
      }

      @Override
      public String encodeInText() {
        return v;
      }
    };
  }

  public static PgValue createDate(Date v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        // Days since 1970.1.1.
        int epochDays = (int) v.toLocalDate().toEpochDay();
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(epochDays).array();
      }

      @Override
      public String encodeInText() {
        return v.toString();
      }
    };
  }

  public static PgValue createDate(int v) {
    // Note: v represents number of days since 1970-01-01.
    return createDate(Date.valueOf(LocalDate.ofEpochDay(v)));
  }

  public static PgValue createTime(long v) {
    return createTime(Instant.ofEpochMilli(v / 1_000).atZone(ZoneId.of("Z")));
  }

  private static PgValue createTime(ZonedDateTime v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        long epochMs = v.toInstant().toEpochMilli();
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(epochMs).array();
      }

      @Override
      public String encodeInText() {
        return v.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      }
    };
  }

  public static PgValue createTimestamp(long v) {
    return createTimestamp(Instant.ofEpochMilli(v / 1000).atZone(ZoneId.of("Z")));
  }

  public static PgValue createTimestamp(Timestamp v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        long epochMs = v.getTime();
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(epochMs).array();
      }

      @Override
      public String encodeInText() {
        return v.toInstant().atZone(ZoneId.of("Z")).toOffsetDateTime().toString();
      }
    };
  }

  private static PgValue createTimestamp(ZonedDateTime v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        long epochMs = v.toInstant().toEpochMilli();
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(epochMs).array();
      }

      @Override
      public String encodeInText() {
        return v.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
      }
    };
  }

  public static PgValue createTimestampz(long v) {
    var dateTimeWithOffset =
        Instant.ofEpochMilli(v / 1_000).atZone(ZoneId.systemDefault()).toOffsetDateTime();
    return createTimestampz(dateTimeWithOffset);
  }

  private static PgValue createTimestampz(OffsetDateTime v) {
    return new PgValue() {
      @Override
      public byte[] encodeInBinary() {
        long epochMs = v.toEpochSecond();
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(epochMs).array();
      }

      @Override
      public String encodeInText() {
        return v.toString();
      }
    };
  }
}
