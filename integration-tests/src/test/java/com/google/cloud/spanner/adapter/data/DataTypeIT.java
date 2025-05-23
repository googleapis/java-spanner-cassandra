/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.google.cloud.spanner.adapter.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.data.SettableByIndex;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.cloud.spanner.adapter.AbstractIT;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class DataTypeIT extends AbstractIT {

  private Map<DataType, String> typeToColumnName = new HashMap<>();
  private static final AtomicInteger keyCounter = new AtomicInteger();
  private static final String tableName = "data_types_it";

  // private static final DatabaseContext dbContext;

  public DataTypeIT(DatabaseContext db) {
    super(db);
    // this.dbContext = db;
  }

  @Rule public TestName name = new TestName();

  // @DataProvider annotation removed
  public static Object[][] primitiveTypeSamples() {
    InetAddress address;
    try {
      address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    } catch (UnknownHostException uhae) {
      throw new AssertionError("Could not get address from 127.0.0.1", uhae);
    }

    return new Object[][] {
      new Object[] {DataTypes.ASCII, "ascii"},
      new Object[] {DataTypes.BIGINT, Long.MAX_VALUE},
      new Object[] {DataTypes.BIGINT, null, 0L},
      new Object[] {DataTypes.BLOB, Bytes.fromHexString("0xCAFE")},
      new Object[] {DataTypes.BOOLEAN, Boolean.TRUE},
      new Object[] {DataTypes.BOOLEAN, null, false},
      new Object[] {DataTypes.DECIMAL, new BigDecimal("12.3E+7")},
      new Object[] {DataTypes.DOUBLE, Double.MAX_VALUE},
      new Object[] {DataTypes.DOUBLE, null, 0.0},
      new Object[] {DataTypes.FLOAT, Float.MAX_VALUE},
      new Object[] {DataTypes.FLOAT, null, 0.0f},
      new Object[] {DataTypes.INET, address},
      new Object[] {DataTypes.TINYINT, Byte.MAX_VALUE},
      new Object[] {DataTypes.TINYINT, null, (byte) 0},
      new Object[] {DataTypes.SMALLINT, Short.MAX_VALUE},
      new Object[] {DataTypes.SMALLINT, null, (short) 0},
      new Object[] {DataTypes.INT, Integer.MAX_VALUE},
      new Object[] {DataTypes.INT, null, 0},
      new Object[] {DataTypes.TEXT, "text"},
      new Object[] {DataTypes.TIMESTAMP, Instant.ofEpochMilli(872835240000L)},
      new Object[] {DataTypes.DATE, LocalDate.ofEpochDay(16071)},
      new Object[] {DataTypes.TIME, LocalTime.ofNanoOfDay(54012123450000L)},
      new Object[] {DataTypes.TIMEUUID, UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66")},
      new Object[] {DataTypes.UUID, UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00")},
      new Object[] {DataTypes.VARINT, new BigInteger(Integer.toString(Integer.MAX_VALUE) + "000")}
    };
  }

  @SuppressWarnings({"unchecked", "rawtypes"}) // Retained as method creates raw collections
  public static Object[][] typeSamples() {
    Object[][] primitiveSamples = primitiveTypeSamples();

    return Arrays.stream(primitiveSamples)
        .flatMap(
            o -> {
              List<Object[]> samples = new ArrayList<>();
              samples.add(o);

              if (o[1] == null) {
                return samples.stream();
              }

              DataType dataType = (DataType) o[0];

              ListType listType = new DefaultListType(dataType, false);
              List data = Collections.singletonList(o[1]);
              samples.add(new Object[] {listType, data});

              if (dataType != DataTypes.DURATION) {
                SetType setType = new DefaultSetType(dataType, false);
                Set s = Collections.singleton(o[1]);
                samples.add(new Object[] {setType, s});
              }

              MapType mapOfTypeElement = new DefaultMapType(DataTypes.INT, dataType, false);
              Map mElement = new HashMap();
              mElement.put(0, o[1]);
              samples.add(new Object[] {mapOfTypeElement, mElement});

              if (dataType != DataTypes.DURATION) {
                MapType mapOfTypeKey = new DefaultMapType(dataType, DataTypes.INT, false);
                Map mKey = new HashMap();
                mKey.put(o[1], 0);
                samples.add(new Object[] {mapOfTypeKey, mKey});
              }

              return samples.stream();
            })
        .toArray(Object[][]::new);
  }

  private static final Map<String, String> cqlToSpannerTypeMap = new HashMap<>();

  static {
    cqlToSpannerTypeMap.put("tinyint", "INT64");
    cqlToSpannerTypeMap.put("smallint", "INT64");
    cqlToSpannerTypeMap.put("int", "INT64");
    cqlToSpannerTypeMap.put("bigint", "INT64");
    cqlToSpannerTypeMap.put("float", "FLOAT32");
    cqlToSpannerTypeMap.put("double", "FLOAT64");
    cqlToSpannerTypeMap.put("decimal", "NUMERIC");
    cqlToSpannerTypeMap.put("varint", "NUMERIC");
    cqlToSpannerTypeMap.put("text", "STRING(MAX)");
    cqlToSpannerTypeMap.put("varchar", "STRING(MAX)");
    cqlToSpannerTypeMap.put("ascii", "STRING(MAX)");
    cqlToSpannerTypeMap.put("uuid", "STRING(MAX)");
    cqlToSpannerTypeMap.put("inet", "STRING(MAX)");
    cqlToSpannerTypeMap.put("timeuuid", "STRING(MAX)");
    cqlToSpannerTypeMap.put("date", "DATE");
    cqlToSpannerTypeMap.put("time", "INT64");
    cqlToSpannerTypeMap.put("timestamp", "TIMESTAMP");
    cqlToSpannerTypeMap.put("boolean", "BOOL");
    cqlToSpannerTypeMap.put("blob", "BYTES(MAX)");
    cqlToSpannerTypeMap.put("counter", "INT64");
  }

  @Before
  public void createTable() throws Exception {
    int counter = 0;
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("k", new ColumnDefinition("INT64", "INT", true));
    Map<DataType, String> localTypeToColumnName = new HashMap<>();
    for (Object[] sample : typeSamples()) {
      DataType dataType = (DataType) sample[0];
      if (!localTypeToColumnName.containsKey(dataType)) {
        int columnIndex = ++counter;
        String columnName = "column_" + columnIndex;
        localTypeToColumnName.put(dataType, columnName);
        String cqlType = typeForCql(dataType);
        testColumns.put(
            columnName, new ColumnDefinition(convertCqlToSpannerType(cqlType), cqlType, false));
      }
    }
    TableDefinition test = new TableDefinition(tableName, testColumns);
    typeToColumnName = new HashMap<>(localTypeToColumnName);
    db.createTables(test);
  }

  // @UseDataProvider annotation removed
  @Test
  public void should_insert_non_primary_key_column_simple_statement_using_format() {
    Object[][] allSamples = typeSamples();

    for (Object[] sample : allSamples) {
      DataType dataType = (DataType) sample[0];
      // skip decimal and float type due to different precisions.
      if (typeForCql(dataType).contains("decimal")) {
        continue;
      }
      if (typeForCql(dataType).contains("float")) {
        continue;
      }
      Object value = sample[1];
      // expectedPrimitiveValue is Object. If sample has 3 elements, it's the default for null.
      Object expectedPrimitiveValue = (sample.length > 2) ? sample[2] : null;

      TypeCodec<Object> codec = db.getSession().getContext().getCodecRegistry().codecFor(dataType);

      int key = nextKey();
      String columnName = columnNameFor(dataType);

      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "INSERT INTO %s (k, %s) values (?, %s)",
                      tableName, columnName, codec.format(value)))
              .addPositionalValue(key)
              .build();

      db.getSession().execute(insert);

      SimpleStatement select =
          SimpleStatement.builder(
                  String.format("SELECT %s FROM %s where k=?", columnName, tableName))
              .addPositionalValue(key)
              .build();
      readValue(select, dataType, value, expectedPrimitiveValue);
    }
  }

  @Test
  public void should_insert_non_primary_key_column_simple_statement_positional_value() {
    Object[][] allSamples = typeSamples();

    for (Object[] sample : allSamples) {
      DataType dataType = (DataType) sample[0];
      // skip decimal and float type due to different precisions.
      if (typeForCql(dataType).contains("decimal")) {
        continue;
      }
      if (typeForCql(dataType).contains("float")) {
        continue;
      }
      Object value = sample[1];
      // expectedPrimitiveValue is Object. If sample has 3 elements, it's the default for null.
      Object expectedPrimitiveValue = (sample.length > 2) ? sample[2] : null;

      int key = nextKey();
      String columnName = columnNameFor(dataType);

      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format("INSERT INTO %s (k, %s) values (?, ?)", tableName, columnName))
              .addPositionalValues(key, value)
              .build();

      db.getSession().execute(insert);

      SimpleStatement select =
          SimpleStatement.builder(
                  String.format("SELECT %s FROM %s where k=?", columnName, tableName))
              .addPositionalValue(key)
              .build();

      readValue(select, dataType, value, expectedPrimitiveValue);
    }
  }

  @Test
  public void should_insert_non_primary_key_column_bound_statement_positional_value() {
    Object[][] allSamples = typeSamples();

    for (Object[] sample : allSamples) {
      DataType dataType = (DataType) sample[0];
      if (typeForCql(dataType).contains("decimal")) {
        continue;
      }
      if (typeForCql(dataType).contains("float")) {
        continue;
      }
      Object value = sample[1];
      Object expectedPrimitiveValue = (sample.length > 2) ? sample[2] : null;

      int key = nextKey();
      String columnName = columnNameFor(dataType);

      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format("INSERT INTO %s (k, %s) values (?, ?)", tableName, columnName))
              .build();

      PreparedStatement preparedInsert = db.getSession().prepare(insert);
      BoundStatementBuilder boundBuilder = preparedInsert.boundStatementBuilder();
      boundBuilder = setValue(0, boundBuilder, DataTypes.INT, key);
      boundBuilder = setValue(1, boundBuilder, dataType, value);
      BoundStatement boundInsert = boundBuilder.build();
      db.getSession().execute(boundInsert);

      SimpleStatement select =
          SimpleStatement.builder(
                  String.format("SELECT %s FROM %s where k=?", columnName, tableName))
              .build();

      PreparedStatement preparedSelect = db.getSession().prepare(select);
      BoundStatement boundSelect = setValue(0, preparedSelect.bind(), DataTypes.INT, key);

      readValue(boundSelect, dataType, value, expectedPrimitiveValue);
    }
  }

  private String typeForCql(DataType dataType) {
    return dataType.asCql(true, true);
  }

  private String columnNameFor(DataType dataType) {
    return typeToColumnName.get(dataType);
  }

  private static int nextKey() {
    return keyCounter.incrementAndGet();
  }

  public static String convertCqlToSpannerType(String cqlType) {
    if (cqlType == null || cqlType.trim().isEmpty()) {
      return "Unsupported";
    }
    String cqlTypeLower = cqlType.trim().toLowerCase();
    Pattern collectionPattern = Pattern.compile("^(set|list|map)<(.+)>$");
    Matcher matcher = collectionPattern.matcher(cqlTypeLower);

    if (matcher.matches()) {
      String collectionName = matcher.group(1);
      String innerTypesStr = matcher.group(2);
      if ("set".equals(collectionName) || "list".equals(collectionName)) {
        String innerSpannerType = convertCqlToSpannerType(innerTypesStr.trim());
        if ("Unsupported".equals(innerSpannerType)) {
          return "Unsupported";
        }
        return "ARRAY<" + innerSpannerType + ">";
      } else if ("map".equals(collectionName)) {
        String[] mapTypes = innerTypesStr.split(",", 2);
        if (mapTypes.length == 2) {
          // All maps are json
          return "JSON";
        } else {
          return "Unsupported";
        }
      }
    }
    if (cqlTypeLower.startsWith("frozen<") && cqlTypeLower.endsWith(">")) {
      String innerFrozenType =
          cqlTypeLower.substring("frozen<".length(), cqlTypeLower.length() - 1);
      return convertCqlToSpannerType(innerFrozenType);
    }
    return cqlToSpannerTypeMap.getOrDefault(cqlTypeLower, "Unsupported");
  }

  private <S extends SettableByIndex<S>> S setValue(
      int index, S bs, DataType dataType, Object value) {
    TypeCodec<Object> codec = db.getSession().getContext().getCodecRegistry().codecFor(dataType);

    if (value == null) {
      return bs.setToNull(index);
    }

    switch (dataType.getProtocolCode()) {
      case ProtocolConstants.DataType.ASCII:
      case ProtocolConstants.DataType.VARCHAR:
        bs = bs.setString(index, (String) value);
        break;
      case ProtocolConstants.DataType.BIGINT:
        bs = bs.setLong(index, (long) value);
        break;
      case ProtocolConstants.DataType.BLOB:
        bs = bs.setByteBuffer(index, (ByteBuffer) value);
        break;
      case ProtocolConstants.DataType.BOOLEAN:
        bs = bs.setBoolean(index, (boolean) value);
        break;
      case ProtocolConstants.DataType.DECIMAL:
        bs = bs.setBigDecimal(index, (BigDecimal) value);
        break;
      case ProtocolConstants.DataType.DOUBLE:
        bs = bs.setDouble(index, (double) value);
        break;
      case ProtocolConstants.DataType.FLOAT:
        bs = bs.setFloat(index, (float) value);
        break;
      case ProtocolConstants.DataType.INET:
        bs = bs.setInetAddress(index, (InetAddress) value);
        break;
      case ProtocolConstants.DataType.TINYINT:
        bs = bs.setByte(index, (byte) value);
        break;
      case ProtocolConstants.DataType.SMALLINT:
        bs = bs.setShort(index, (short) value);
        break;
      case ProtocolConstants.DataType.INT:
        bs = bs.setInt(index, (int) value);
        break;
      case ProtocolConstants.DataType.TIMESTAMP:
        bs = bs.setInstant(index, (Instant) value);
        break;
      case ProtocolConstants.DataType.DATE:
        bs = bs.setLocalDate(index, (LocalDate) value);
        break;
      case ProtocolConstants.DataType.TIME:
        bs = bs.setLocalTime(index, (LocalTime) value);
        break;
      case ProtocolConstants.DataType.TIMEUUID:
      case ProtocolConstants.DataType.UUID:
        bs = bs.setUuid(index, (UUID) value);
        break;
      case ProtocolConstants.DataType.VARINT:
        bs = bs.setBigInteger(index, (BigInteger) value);
        break;
      // fall through
      case ProtocolConstants.DataType.LIST:
      case ProtocolConstants.DataType.SET:
      case ProtocolConstants.DataType.MAP:
        bs = bs.set(index, value, codec);
        break;
      default:
        fail("Unhandled DataType " + dataType);
    }
    return bs;
  }

  private void readValue(
      Statement<?> select, DataType dataType, Object value, Object expectedPrimitiveValue) {
    TypeCodec<Object> codec = db.getSession().getContext().getCodecRegistry().codecFor(dataType);
    ResultSet result = db.getSession().execute(select);

    String columnName = columnNameFor(dataType);

    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();

    Object expectedValue = expectedPrimitiveValue != null ? expectedPrimitiveValue : value;

    switch (dataType.getProtocolCode()) {
      case ProtocolConstants.DataType.ASCII:
      case ProtocolConstants.DataType.VARCHAR:
        assertThat(row.getString(columnName)).isEqualTo(expectedValue);
        assertThat(row.getString(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.BIGINT:
        assertThat(row.getLong(columnName)).isEqualTo(expectedValue);
        assertThat(row.getLong(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.BLOB:
        assertThat(row.getByteBuffer(columnName)).isEqualTo(expectedValue);
        assertThat(row.getByteBuffer(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.BOOLEAN:
        assertThat(row.getBoolean(columnName)).isEqualTo(expectedValue);
        assertThat(row.getBoolean(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.DECIMAL:
        assertThat(row.getBigDecimal(columnName)).isEqualTo(expectedValue);
        assertThat(row.getBigDecimal(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.DOUBLE:
        assertThat(row.getDouble(columnName)).isEqualTo(expectedValue);
        assertThat(row.getDouble(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.FLOAT:
        assertThat(row.getFloat(columnName)).isEqualTo(expectedValue);
        assertThat(row.getFloat(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.INET:
        assertThat(row.getInetAddress(columnName)).isEqualTo(expectedValue);
        assertThat(row.getInetAddress(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.TINYINT:
        assertThat(row.getByte(columnName)).isEqualTo(expectedValue);
        assertThat(row.getByte(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.SMALLINT:
        assertThat(row.getShort(columnName)).isEqualTo(expectedValue);
        assertThat(row.getShort(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.INT:
        assertThat(row.getInt(columnName)).isEqualTo(expectedValue);
        assertThat(row.getInt(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.DURATION:
        assertThat(row.getCqlDuration(columnName)).isEqualTo(expectedValue);
        assertThat(row.getCqlDuration(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.TIMESTAMP:
        assertThat(row.getInstant(columnName)).isEqualTo(expectedValue);
        assertThat(row.getInstant(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.DATE:
        assertThat(row.getLocalDate(columnName)).isEqualTo(expectedValue);
        assertThat(row.getLocalDate(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.TIME:
        assertThat(row.getLocalTime(columnName)).isEqualTo(expectedValue);
        assertThat(row.getLocalTime(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.TIMEUUID:
      case ProtocolConstants.DataType.UUID:
        assertThat(row.getUuid(columnName)).isEqualTo(expectedValue);
        assertThat(row.getUuid(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.VARINT:
        assertThat(row.getBigInteger(columnName)).isEqualTo(expectedValue);
        assertThat(row.getBigInteger(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.LIST:
      case ProtocolConstants.DataType.MAP:
      case ProtocolConstants.DataType.SET:
        assertThat(row.get(columnName, codec)).isEqualTo(expectedValue);
        assertThat(row.get(0, codec)).isEqualTo(expectedValue);
        break;
      default:
        fail("Unhandled DataType " + dataType);
    }

    if (value == null) {
      assertThat(row.isNull(columnName)).isTrue();
      assertThat(row.isNull(0)).isTrue();
    }

    ProtocolVersion protocolVersion = db.getSession().getContext().getProtocolVersion();
    assertThat(codec.decode(row.getBytesUnsafe(columnName), protocolVersion)).isEqualTo(value);
    assertThat(codec.decode(row.getBytesUnsafe(0), protocolVersion)).isEqualTo(value);
  }
}
