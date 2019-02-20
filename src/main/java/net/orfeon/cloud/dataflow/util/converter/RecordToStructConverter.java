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
import java.util.ArrayList;
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
        final boolean isNullField = record.get(field.name()) == null;
        switch (type.getType()) {
            case STRING:
                return builder.set(field.name()).to(isNullField ? null : record.get(field.name()).toString());
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                final ByteBuffer bytes = (ByteBuffer)record.get(field.name());
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    final String strValue = convertNumericBytesToString(bytes.array(), scale);
                    return builder.set(field.name()).to(isNullField ? null : strValue);
                }
                return builder.set(field.name()).to(isNullField ? null : ByteArray.copyFrom(bytes));
            case ENUM:
                return builder.set(field.name()).to(isNullField ? null : record.get(field.name()).toString());
            case INT:
                final Long intvalue = isNullField ? null : new Long((Integer)record.get(field.name()));
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    if(intvalue == null) {
                        return builder.set(field.name()).to((Date)null);
                    }
                    final LocalDate ld = LocalDate.ofEpochDay(intvalue);
                    final Date date = Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
                    return builder.set(field.name()).to(date);
                }
                return builder.set(field.name()).to(intvalue);
            case LONG:
                final Long longvalue = (Long)record.get(field.name());
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    if(longvalue == null) {
                        return builder.set(field.name()).to((Timestamp)null);
                    }
                    final Long microseconds = type.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longvalue : longvalue * 1000;
                    return builder.set(field.name()).to(Timestamp.ofTimeMicroseconds(microseconds));
                }
                return builder.set(field.name()).to(longvalue);
            case FLOAT:
                return builder.set(field.name()).to((Double)record.get(field.name()));
            case DOUBLE:
                return builder.set(field.name()).to((Double)record.get(field.name()));
            case BOOLEAN:
                return builder.set(field.name()).to((Boolean)record.get(field.name()));
            case FIXED:
                return builder.set(field.name()).to((String)record.get(field.name()));
            case RECORD:
                final Struct childStruct = convert((GenericRecord)record.get(field.name()));
                return builder.set(field.name()).to(childStruct);
            case MAP:
                return builder;
            case UNION:
                for(final Schema childSchema : field.schema().getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setFieldValue(builder, field, childSchema, record);
                }
                return builder;
            case ARRAY:
                if (Schema.Type.UNION.equals(field.schema().getType())) {
                    for(final Schema childSchema : field.schema().getTypes()) {
                        if (Schema.Type.NULL.equals(childSchema.getType())) {
                            continue;
                        }
                        return setArrayFieldValue(builder, field, childSchema.getElementType(), record);
                    }
                } else {
                    return setArrayFieldValue(builder, field, field.schema().getElementType(), record);
                }
            case NULL:
                return builder;
            default:
                return builder;
        }
    }

    private static Struct.Builder setArrayFieldValue(Struct.Builder builder, Schema.Field field, Schema type, GenericRecord record) {
        switch (type.getType()) {
            case STRING:
                if(record.get(field.name()) == null) {
                    return builder.set(field.name()).toStringArray(null);
                }
                final List<String> stringList = new ArrayList<>();
                for(final Object obj : (List<String>) record.get(field.name())) {
                    stringList.add(obj == null ? null : obj.toString());
                }
                return builder.set(field.name()).toStringArray(stringList);
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    return builder.set(field.name()).toStringArray(((List<ByteBuffer>)record.get(field.name()))
                            .stream()
                            .map(bytes -> convertNumericBytesToString(bytes.array(), scale))
                            .collect(Collectors.toList()));
                }
                return builder.set(field.name()).toBytesArray(((List<ByteBuffer>)record.get(field.name()))
                        .stream()
                        .map(ByteArray::copyFrom)
                        .collect(Collectors.toList()));
            case ENUM:
                return builder.set(field.name()).toStringArray((List<String>) record.get(field.name()));
            case INT:
                final List<Integer> intvalues =  ((List<Integer>) record.get(field.name()));
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    if(intvalues == null) {
                        return builder.set(field.name()).toDateArray(null);
                    }
                    final List<Date> dateList = new ArrayList<>();
                    for(final Integer days : intvalues) {
                        if(days == null) {
                            dateList.add(null);
                            continue;
                        }
                        final LocalDate ld = LocalDate.ofEpochDay(days);
                        final Date date = Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
                        dateList.add(date);
                    }
                    return builder.set(field.name()).toDateArray(dateList);
                } else {
                    final List<Long> emptyArray = new ArrayList<>();
                    return builder.set(field.name()).toInt64Array(emptyArray);
                }
            case LONG:
                final List<Long> longvalues = (List<Long>)record.get(field.name());
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    if(longvalues == null) {
                        return builder.set(field.name()).toTimestampArray(null);
                    }
                    final List<Timestamp> timestampList = new ArrayList<>();
                    for(final Long longvalue : longvalues) {
                        if(longvalue == null) {
                            timestampList.add(null);
                            continue;
                        }
                        final Long epochmicros = type.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longvalue : longvalue * 1000;
                        final Timestamp timestamp = Timestamp.ofTimeMicroseconds(epochmicros);
                        timestampList.add(timestamp);
                    }
                    return builder.set(field.name()).toTimestampArray(timestampList);
                } else {
                    return builder.set(field.name()).toInt64Array(longvalues);
                }
            case FLOAT:
                return builder.set(field.name()).toFloat64Array((List<Double>) record.get(field.name()));
            case DOUBLE:
                return builder.set(field.name()).toFloat64Array((List<Double>)record.get(field.name()));
            case BOOLEAN:
                return builder.set(field.name()).toBoolArray((List<Boolean>)record.get(field.name()));
            case FIXED:
                return builder.set(field.name()).toStringArray((List<String>)record.get(field.name()));
            case RECORD:
                // Currently, Not support conversion from nested avro record. (Only consider avro file by SpannerToAvro)
                //List<GenericRecord> records = (List<GenericRecord>)record.get(field.name());
                //List<Struct> structList = new ArrayList<>();
                //for(GenericRecord childRecord : records) {
                //    final Struct childStruct = convertStruct(childRecord);
                //    structList.add(childStruct);
                //}
                //return binder.toStructArray(structList);
                return builder;
            case MAP:
                return builder;
            case UNION:
                for(final Schema childSchema : field.schema().getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    for(final Schema cchildSchema : childSchema.getElementType().getTypes()) {
                        if(Schema.Type.NULL.equals(cchildSchema.getType())) {
                            continue;
                        }
                        builder = setArrayFieldValue(builder, field, cchildSchema, record);
                        return builder;
                    }
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
