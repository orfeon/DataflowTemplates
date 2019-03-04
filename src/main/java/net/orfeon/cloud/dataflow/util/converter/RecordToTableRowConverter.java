package net.orfeon.cloud.dataflow.util.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Date;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class RecordToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RecordToTableRowConverter.class);

    public static TableRow convert(GenericRecord record) {
        TableRow row = new TableRow();
        for(final Schema.Field field : record.getSchema().getFields()) {
            row = setFieldValue(row, field, field.schema(), record);
        }
        return row;
    }

    public static TableRow convert(SchemaAndRecord record) {
        return convert(record.getRecord());
    }


    private static TableRow setFieldValue(TableRow row, Schema.Field field, Schema type, GenericRecord record) {
        if(record.get(field.name()) == null) {
            return row.set(field.name(), null);
        }
        switch (type.getType()) {
            case ENUM:
            case STRING:
                return row.set(field.name(), record.get(field.name()).toString());
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    final ByteBuffer bytes = (ByteBuffer)record.get(field.name());
                    final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes.array()).longValue(), scale);
                    return row.set(field.name(), bigDecimal);
                }
                final ByteBuffer bytes = (ByteBuffer)record.get(field.name());
                return row.set(field.name(), bytes);
            case INT:
                final Long intvalue = new Long((Integer)record.get(field.name()));
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    final LocalDate ld = LocalDate.ofEpochDay(intvalue);
                    //final Date date = Date.fromYearMonthDay(ld.getYear() <= 1 ? 1 : ld.getYear() >= 24356 ? 24356 : ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
                    final Date date = Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
                    return row.set(field.name(), date);
                }
                return row.set(field.name(), intvalue);
            case LONG:
                final Long longvalue = (Long)record.get(field.name());
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    final Long seconds = type.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longvalue / 1000000 : longvalue / 1000;
                    return row.set(field.name(), seconds);
                }
                return row.set(field.name(), longvalue);
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return row.set(field.name(), record.get(field.name()));
            case FIXED:
                final GenericData.Fixed fixed = (GenericData.Fixed)record.get(field.name());
                return row.set(field.name(), fixed.bytes());
            case RECORD:
                final TableRow childRow = convert((GenericRecord)record.get(field.name()));
                return row.set(field.name(), childRow);
            case MAP:
                final List<TableRow> childMapRows = new ArrayList<>();
                final Map<Object, Object> map = (Map)record.get(field.name());
                for(Map.Entry<Object, Object> entry : map.entrySet()) {
                    final TableRow childMapRow = new TableRow();
                    childMapRow.set("key", entry.getKey() == null ? "" : entry.getKey().toString());
                    childMapRow.set("value", convertValue(type.getValueType().getType(), entry.getValue()));
                    childMapRows.add(childMapRow);
                }
                return row.set(field.name(), childMapRows);
            case UNION:
                for(final Schema childSchema : type.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setFieldValue(row, field, childSchema, record);
                }
                return row;
            case ARRAY:
                return setArrayFieldValue(row, field, type.getElementType(), record);
            case NULL:
                // BigQuery ignores NULL value
                // https://cloud.google.com/bigquery/data-formats#avro_format
                return row;
            default:
                return row;
        }
    }

    private static TableRow setArrayFieldValue(TableRow row, Schema.Field field, Schema type, GenericRecord record) {
        boolean isNull = record.get(field.name()) == null;
        switch (type.getType()) {
            case ENUM:
            case STRING:
                return row.set(field.name(), isNull ? null : ((List<Object>)record.get(field.name()))
                        .stream()
                        .filter(utf8 -> utf8 != null)
                        .map(utf8 -> utf8.toString())
                        .collect(Collectors.toList()));
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    return row.set(field.name(), isNull ? null : ((List<ByteBuffer>)record.get(field.name()))
                            .stream()
                            .filter(bytes -> bytes != null)
                            .map(bytes -> BigDecimal.valueOf(new BigInteger(bytes.array()).longValue(), scale))
                            .collect(Collectors.toList()));
                }
                return row.set(field.name(), ((List<ByteBuffer>)record.get(field.name())).stream()
                        .filter(bytes -> bytes != null)
                        .collect(Collectors.toList()));
            case INT:
                final List<Integer> intValues =  ((List<Integer>) record.get(field.name()));
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    final List<Date> dateList = new ArrayList<>();
                    for(final Integer days : intValues) {
                        if(days == null) {
                            continue; // skip when null value (BigQuery disallow to set null value in array.)
                        }
                        final LocalDate ld = LocalDate.ofEpochDay(days);
                        final Date date = Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
                        dateList.add(date);
                    }
                    return row.set(field.name(), isNull ? null : dateList);
                } else {
                    return row.set(field.name(), intValues.stream()
                            .filter(i -> i != null)
                            .map(i -> i.longValue()).collect(Collectors.toList()));
                }
            case LONG:
                final List<Long> longValues = (List<Long>)record.get(field.name());
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    final List<Long> timestampList = new ArrayList<>();
                    for(final Long longvalue : longValues) {
                        if(longvalue == null) {
                            continue; // skip null value (BigQuery do not support null value in array.)
                        }
                        final Long seconds = type.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longvalue / 1000000 : longvalue / 1000;
                        timestampList.add(seconds);
                    }
                    return row.set(field.name(), isNull ? null : timestampList);
                } else {
                    return row.set(field.name(), isNull ? null : longValues.stream()
                            .filter(l -> l != null)
                            .collect(Collectors.toList()));
                }
            case FLOAT:
                return row.set(field.name(), isNull ? null : ((List<Float>)record.get(field.name())).stream()
                        .filter(f -> f != null)
                        .collect(Collectors.toList()));
            case DOUBLE:
                return row.set(field.name(), isNull ? null : ((List<Double>)record.get(field.name())).stream()
                        .filter(d -> d != null)
                        .collect(Collectors.toList()));
            case BOOLEAN:
                return row.set(field.name(), isNull ? null : ((List<Boolean>)record.get(field.name())).stream()
                        .filter(b -> b != null)
                        .collect(Collectors.toList()));
            case FIXED:
                return row.set(field.name(), isNull ? null : ((List<GenericData.Fixed>)record.get(field.name()))
                        .stream()
                        .filter(s -> s != null)
                        .map(s -> s.bytes())
                        .collect(Collectors.toList()));
            case RECORD:
                return row.set(field.name(), isNull ? null : ((List<GenericRecord>)record.get(field.name()))
                        .stream()
                        .filter(r -> r != null)
                        .map(r -> convert(r))
                        .collect(Collectors.toList()));
            case MAP:
                // MAP in Array is not considerable ??
                row.set(field.name(), isNull ? null : ((List<Map<Object,Object>>)record.get(field.name()))
                        .stream()
                        .filter(m -> m != null)
                        .map(m -> m.entrySet().stream()
                                .map(e -> {
                                    final TableRow childMapRow = new TableRow();
                                    childMapRow.set("key", e.getKey() == null ? "" : e.getKey().toString());
                                    childMapRow.set("value", convertValue(type.getValueType().getType(), e.getValue()));
                                    return childMapRow;
                                })
                                .collect(Collectors.toList()))
                        .collect(Collectors.toList()));
            case UNION:
                for(final Schema childSchema : type.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setArrayFieldValue(row, field, childSchema, record);
                }
                return row;
            case ARRAY:
                // BigQuery does not support nested array.
                // https://cloud.google.com/bigquery/data-formats#avro_format
                return row;
            case NULL:
                // BigQuery ignore NULL value
                // https://cloud.google.com/bigquery/data-formats#avro_format
                return row;
            default:
                return row;
        }

    }

    private static Object convertValue(Schema.Type type, Object value) {
        switch(type) {
            case ENUM:
            case STRING:
                return value == null ? null : value.toString();
            case INT:
                return value == null ? null : (Integer)value;
            case LONG:
                return value == null ? null : (Long)value;
            case FLOAT:
                return value == null ? null : (Float)value;
            case DOUBLE:
                return value == null ? null : (Double)value;
            case BOOLEAN:
                return value == null ? null : (Boolean)value;
            case BYTES:
                return value == null ? null : (ByteBuffer)value;
            default:
                return value;
        }
    }

}
