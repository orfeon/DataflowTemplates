package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

public class RecordToStructConverter {

    private RecordToStructConverter() {

    }

    public static Struct convert(GenericRecord record) {
        Struct.Builder builder = Struct.newBuilder();
        for(final Schema.Field field : record.getSchema().getFields()) {
            builder = setFieldValue(builder, field, field.schema(), record);
        }
        return builder.build();
    }

    public static Struct convert(SchemaAndRecord record) {
        return convert(record.getRecord());
    }

    private static Struct.Builder setFieldValue(Struct.Builder builder, Schema.Field field, Schema type, GenericRecord record) {
        final Object value = record.get(field.name());
        final boolean isNullField = value == null;
        switch (type.getType()) {
            case ENUM:
            case STRING:
                return builder.set(field.name()).to(isNullField ? null : value.toString());
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                final ByteBuffer bytes = (ByteBuffer) value;
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    final String strValue = isNullField ? null : convertNumericBytesToString(bytes.array(), scale);
                    return builder.set(field.name()).to(strValue);
                }
                return builder.set(field.name()).to(isNullField ? null : ByteArray.copyFrom(bytes));
            case INT:
                final Long intValue = isNullField ? null : new Long((Integer) value);
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    return builder.set(field.name()).to(convertIntegerToDate(intValue));
                }
                return builder.set(field.name()).to(intValue);
            case LONG:
                final Long longValue = (Long) value;
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    return builder.set(field.name()).to(convertLongToTimestamp(type, longValue));
                }
                return builder.set(field.name()).to(longValue);
            case FLOAT:
                return builder.set(field.name()).to((Float) value);
            case DOUBLE:
                return builder.set(field.name()).to((Double) value);
            case BOOLEAN:
                return builder.set(field.name()).to((Boolean) value);
            case FIXED:
                return builder.set(field.name()).to(isNullField ? null : value.toString());
            case RECORD:
                final Struct childStruct = convert((GenericRecord) value);
                return builder.set(field.name()).to(childStruct);
            case MAP:
                return builder;
            case UNION:
                for(final Schema childSchema : type.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setFieldValue(builder, field, childSchema, record);
                }
                return builder;
            case ARRAY:
                return setArrayFieldValue(builder, field, type.getElementType(), record);
            case NULL:
                return builder;
            default:
                return builder;
        }
    }

    private static Struct.Builder setArrayFieldValue(Struct.Builder builder, Schema.Field field, Schema type, GenericRecord record) {
        final Object value = record.get(field.name());
        final boolean isNullField = value == null;
        switch (type.getType()) {
            case ENUM:
            case STRING:
                return builder.set(field.name()).toStringArray(isNullField ? null :
                        ((List<Object>) value).stream()
                                .map(utf8 -> utf8 == null ? null : utf8.toString())
                                .collect(Collectors.toList()));
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    return builder.set(field.name()).toStringArray(isNullField ? null :
                            ((List<ByteBuffer>) value).stream()
                                    .map(bytes -> bytes == null ? null : convertNumericBytesToString(bytes.array(), scale))
                                    .collect(Collectors.toList()));
                }
                return builder.set(field.name()).toBytesArray(isNullField ? null :
                        ((List<ByteBuffer>) value).stream()
                                .map(bytes -> bytes == null ? null : ByteArray.copyFrom(bytes))
                                .collect(Collectors.toList()));
            case INT:
                final List<Integer> intValues =  ((List<Integer>) value);
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    return builder.set(field.name()).toDateArray(isNullField ? null : intValues.stream()
                            .map(days -> convertIntegerToDate(new Long(days)))
                            .collect(Collectors.toList()));
                } else {
                    return builder.set(field.name()).toInt64Array(isNullField ? null : intValues.stream()
                            .map(i -> i == null ? null : new Long(i))
                            .collect(Collectors.toList()));
                }
            case LONG:
                final List<Long> longValues = (List<Long>) value;
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    return builder.set(field.name()).toTimestampArray(isNullField ? null : longValues.stream()
                            .map(l -> convertLongToTimestamp(type, l))
                            .collect(Collectors.toList()));
                } else {
                    return builder.set(field.name()).toInt64Array(longValues);
                }
            case FLOAT:
                return builder.set(field.name()).toFloat64Array(isNullField ? null :
                        ((List<Float>) value).stream()
                                .map(f -> f == null ? null : new Double(f))
                                .collect(Collectors.toList()));
            case DOUBLE:
                return builder.set(field.name()).toFloat64Array((List<Double>) value);
            case BOOLEAN:
                return builder.set(field.name()).toBoolArray((List<Boolean>) value);
            case FIXED:
                return builder.set(field.name()).toStringArray((List<String>) value);
            case RECORD:
                // Currently, Not support conversion from nested avro record. (Only consider avro file by SpannerToAvro)
                return builder;
            case MAP:
                return builder;
            case UNION:
                for(final Schema childSchema : type.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setArrayFieldValue(builder, field, childSchema, record);
                }
                return builder;
            case ARRAY:
                // Currently, Not support conversion from nested array record. (Only consider avro file by SpannerToAvro)
                return builder;
            case NULL:
                return builder;
            default:
                return builder;
        }
    }

    private static Date convertIntegerToDate(Long intValue) {
        if(intValue == null) {
            return null;
        }
        final LocalDate ld = LocalDate.ofEpochDay(intValue);
        return Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
    }

    private static Timestamp convertLongToTimestamp(Schema type, Long longValue) {
        if(longValue == null) {
            return null;
        }
        final Long microseconds = type.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longValue : longValue * 1000;
        return Timestamp.ofTimeMicroseconds(microseconds);
    }

    private static String convertNumericBytesToString(byte[] bytes, int scale) {
        BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
        if(scale == 0) {
            return bigDecimal.toPlainString();
        }
        StringBuilder sb = new StringBuilder(bigDecimal.toPlainString());
        while(sb.lastIndexOf("0") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        if(sb.lastIndexOf(".") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }
}
