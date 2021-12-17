package org.astraea.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility related to data and unit, this class dedicated to easing the pain of converting and
 * calculating/displaying/measurement and its unit.
 *
 * @see <a href="https://physics.nist.gov/cuu/Units/binary.html">NIST Reference on Binary Unit</a>
 *     to understand the detail behind those units.
 */
public enum DataUnit {
  Bit(1, false),
  Kb(1000, Bit),
  Mb(1000, Kb),
  Gb(1000, Mb),
  Tb(1000, Gb),
  Pb(1000, Tb),
  Eb(1000, Pb),
  Zb(1000, Eb),
  Yb(1000, Zb),

  Kib(1024, Bit),
  Mib(1024, Kib),
  Gib(1024, Mib),
  Tib(1024, Gib),
  Pib(1024, Tib),
  Eib(1024, Pib),
  Zib(1024, Eib),
  Yib(1024, Zib),

  Byte(8, true, true),
  KB(1000, Byte, true),
  MB(1000, KB, true),
  GB(1000, MB, true),
  TB(1000, GB, true),
  PB(1000, TB, true),
  EB(1000, PB, true),
  ZB(1000, EB, true),
  YB(1000, ZB, true),

  KiB(1024, Byte),
  MiB(1024, KiB),
  GiB(1024, MiB),
  TiB(1024, GiB),
  PiB(1024, TiB),
  EiB(1024, PiB),
  ZiB(1024, EiB),
  YiB(1024, ZiB);

  final BigInteger bits;
  final boolean byteBasedUnit;
  final boolean candidateUnitForToString;

  DataUnit(long bits, boolean isByteBasedUnit) {
    this(bits, isByteBasedUnit, false);
  }

  DataUnit(long bits, boolean isByteBasedUnit, boolean candidateUnitForToString) {
    this.bits = BigInteger.valueOf(bits);
    this.byteBasedUnit = isByteBasedUnit;
    this.candidateUnitForToString = candidateUnitForToString;
  }

  DataUnit(long measurement, DataUnit unit) {
    this(measurement, unit, false);
  }

  DataUnit(long measurement, DataUnit unit, boolean candidateUnitForToString) {
    this.bits = unit.bits.multiply(BigInteger.valueOf(measurement));
    this.byteBasedUnit = unit.byteBasedUnit;
    this.candidateUnitForToString = candidateUnitForToString;
  }

  /**
   * Return a {@link DataSize} based on given measurement and related unit.
   *
   * <pre>{@code
   * DataUnit.KB.of(500);  // 500 KB  (500 * 1000 bytes)
   * DataUnit.KiB.of(500); // 500 KiB (500 * 1024 bytes)
   * DataUnit.Kb.of(500);  // 500 Kb  (500 * 1000 bits)
   * DataUnit.Kib.of(500); // 500 Kib (500 * 1024 bits)
   * }</pre>
   *
   * @param measurement the data size measurement.
   * @return a size object of given measurement under specific data unit.
   */
  public DataSize of(long measurement) {
    return new DataSize(measurement, this);
  }

  /**
   * Return a {@link DataSize} based on given measurement and unit.
   *
   * <pre>{@code
   * DataUnit.of(500, DataUnit.KB);   // 500 KB  (500 * 1000 bytes)
   * DataUnit.of(500, DataUnit.KiB);  // 500 KiB (500 * 1024 bytes)
   * DataUnit.of(500, DataUnit.Kb);   // 500 Kb  (500 * 1000 bits)
   * DataUnit.of(500, DataUnit.Kib);  // 500 Kib (500 * 1024 bits)
   * }</pre>
   *
   * @param measurement the data size measurement.
   * @param unit the data unit of given measurement.
   * @return a size object of given measurement under specific data unit.
   */
  public static DataSize of(long measurement, DataUnit unit) {
    return new DataSize(measurement, unit);
  }

  /**
   * List of recommended unit for display data, {@link DataSize#toString()} take advantage of this
   * collection to find ideal unit for string formalization.
   */
  static final List<DataUnit> BYTE_UNIT_SIZE_ORDERED_LIST =
      Arrays.stream(DataUnit.values())
          .filter(x -> x.candidateUnitForToString)
          .sorted(Comparator.comparing(x -> x.bits))
          .collect(Collectors.toUnmodifiableList());

  /** Data rate class */
  public static class DataRate {
    private final DataSize totalBitTransmitted;
    private final BigDecimal durationInNanoSecond;

    static BigDecimal fromDurationToBigDecimalSafely(Duration duration) {
      // It the given duration is extremely long(like 1000 years), it might overflow the long
      // maximum value in nano second unit.
      if (duration.toSeconds() >= Long.MAX_VALUE / 1e9) {
        return BigDecimal.valueOf(duration.toSeconds())
            .multiply(BigDecimal.valueOf(ChronoUnit.SECONDS.getDuration().toNanos()))
            .add(BigDecimal.valueOf(duration.toNanosPart()));
      } else {
        return BigDecimal.valueOf(duration.toNanos());
      }
    }

    static BigInteger fromDurationToBigIntegerSafely(Duration duration) {
      // It the given duration is extremely long(like 1000 years), it might overflow the long
      // maximum value in nano second unit.
      if (duration.toSeconds() >= Long.MAX_VALUE / 1e9) {
        return BigInteger.valueOf(duration.toSeconds())
            .multiply(BigInteger.valueOf(ChronoUnit.SECONDS.getDuration().toNanos()))
            .add(BigInteger.valueOf(duration.toNanosPart()));
      } else {
        return BigInteger.valueOf(duration.toNanos());
      }
    }

    DataRate(DataSize initialSize, Duration duration) {
      this.totalBitTransmitted = initialSize;
      this.durationInNanoSecond = fromDurationToBigDecimalSafely(duration);
    }

    /** Return the ideal data rate under specific time unit. */
    public BigDecimal idealDataRate(ChronoUnit chronoUnit) {
      return toBigDecimal(idealDataUnit(chronoUnit), chronoUnit);
    }

    /** Return the ideal data unit under specific time unit. */
    public DataUnit idealDataUnit(ChronoUnit chronoUnit) {
      final BigInteger dataRate =
          totalBitTransmitted
              .bits()
              .multiply(fromDurationToBigIntegerSafely(chronoUnit.getDuration()))
              .divide(durationInNanoSecond.toBigInteger());

      return BYTE_UNIT_SIZE_ORDERED_LIST.stream()
          .sorted(Comparator.reverseOrder())
          .dropWhile((x) -> dataRate.compareTo(x.bits) < 0)
          .findFirst()
          .orElse(Byte);
    }

    /**
     * Calculate the data rate in a specific {@link DataUnit} over a specific {@link ChronoUnit}.
     */
    public BigDecimal toBigDecimal(DataUnit dataUnit, ChronoUnit chronoUnit) {
      return toBigDecimal(dataUnit, chronoUnit.getDuration());
    }

    /** Calculate the data rate in a specific {@link DataUnit} over a specific {@link Duration}. */
    public BigDecimal toBigDecimal(DataUnit dataUnit, Duration duration) {
      return this.totalBitTransmitted
          .measurement(dataUnit)
          .multiply(fromDurationToBigDecimalSafely(duration))
          .divide(durationInNanoSecond, MathContext.DECIMAL32);
    }

    /**
     * Create a {@link DataRate} object from data volume and time unit
     *
     * <pre>{@code
     * DataRate.of(100, DataUnit.KB, ChronoUnit.SECONDS);    // 100 KB/s
     * DataRate.of(100, DataUnit.MB, ChronoUnit.MONTHS);     // 50 MB/month
     * }</pre>
     *
     * @return an object {@link DataRate} correspond to given arguments
     */
    public static DataRate of(long measurement, DataUnit dataUnit, ChronoUnit chronoUnit) {
      return of(measurement, dataUnit, chronoUnit.getDuration());
    }

    /**
     * Create a {@link DataRate} object from data volume and time duration
     *
     * <pre>{@code
     * DataRate.of(100, DataUnit.KB, Duration.ofSeconds(1)); // 100 KB/s
     * DataRate.of(100, DataUnit.MB, Duration.ofSeconds(2)); // 50 MB/s
     * }</pre>
     *
     * @return an object {@link DataRate} correspond to given arguments
     */
    public static DataRate of(long measurement, DataUnit dataUnit, Duration duration) {
      return new DataRate(dataUnit.of(measurement), duration);
    }

    /**
     * Create a {@link DataRate} object from data volume and time duration
     *
     * <pre>{@code
     * DataRate.of(DataUnit.KB.of(100), ChronoUnit.SECONDS); // 100 KB/s
     * DataRate.of(DataUnit.MB.of(100), ChronoUnit.SECONDS); // 50 MB/s
     * }</pre>
     *
     * @return an object {@link DataRate} correspond to given arguments
     */
    public static DataRate of(DataSize dataSize, ChronoUnit chronoUnit) {
      return new DataRate(dataSize, chronoUnit.getDuration());
    }

    /**
     * Create a {@link DataRate} object from data volume and time duration
     *
     * <pre>{@code
     * DataRate.of(DataUnit.KB.of(100), Duration.ofSeconds(1)); // 100 KB/s
     * DataRate.of(DataUnit.MB.of(100), Duration.ofSeconds(2)); // 50 MB/s
     * }</pre>
     *
     * @return an object {@link DataRate} correspond to given arguments
     */
    public static DataRate of(DataSize dataSize, Duration duration) {
      return new DataRate(dataSize, duration);
    }

    public static BigDecimal ofBigDecimal(long measurement, DataUnit unit, ChronoUnit time) {
      return ofBigDecimal(unit.of(measurement), unit, time.getDuration());
    }

    public static BigDecimal ofBigDecimal(long measurement, DataUnit unit, Duration time) {
      return ofBigDecimal(unit.of(measurement), unit, time);
    }

    public static BigDecimal ofBigDecimal(DataSize size, DataUnit unit, ChronoUnit time) {
      return ofBigDecimal(size, unit, time.getDuration());
    }

    public static BigDecimal ofBigDecimal(DataSize size, DataUnit unit, Duration time) {
      return of(size, time).toBigDecimal(unit, time);
    }

    public static double ofDouble(long measurement, DataUnit unit, ChronoUnit time) {
      return ofBigDecimal(measurement, unit, time).doubleValue();
    }

    public static double ofDouble(long measurement, DataUnit unit, Duration time) {
      return ofBigDecimal(measurement, unit, time).doubleValue();
    }

    public static double ofDouble(DataSize size, DataUnit unit, ChronoUnit time) {
      return ofBigDecimal(size, unit, time).doubleValue();
    }

    public static double ofDouble(DataSize size, DataUnit unit, Duration time) {
      return ofBigDecimal(size, unit, time).doubleValue();
    }

    private static String chronoName(ChronoUnit chronoUnit) {
      return chronoUnit.name().toLowerCase(Locale.ROOT);
    }

    public String toString(DataUnit dataUnit, ChronoUnit chronoUnit) {
      BigDecimal dataRate = toBigDecimal(dataUnit, chronoUnit).setScale(2, RoundingMode.HALF_EVEN);
      return String.format("%s %s/%s", dataRate, dataUnit.name(), chronoName(chronoUnit));
    }

    public String toString(ChronoUnit chronoUnit) {
      return toString(idealDataUnit(chronoUnit), chronoUnit);
    }

    @Override
    public String toString() {
      return toString(ChronoUnit.SECONDS);
    }
  }
}
