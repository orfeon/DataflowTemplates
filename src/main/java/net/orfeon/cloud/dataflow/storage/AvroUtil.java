package net.orfeon.cloud.dataflow.storage;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AvroUtil {

    private static final MutableDateTime EPOCH_DATETIME = new MutableDateTime(0, DateTimeZone.UTC);
    private static final Schema NULLABLE_STRING = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    private static final Schema NULLABLE_BYTES = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES));
    private static final Schema NULLABLE_BOOLEAN = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN));
    private static final Schema NULLABLE_LONG = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
    private static final Schema NULLABLE_DOUBLE = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE));
    private static final Schema LOGICAL_DATE_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
    private static final Schema LOGICAL_TIMESTAMP_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));


    private AvroUtil() {

    }

    public static Schema convertSchemaFromStruct(Struct struct) {
        SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(Type.StructField field : struct.getType().getStructFields()) {
            schemaFields = addSchemaField(schemaFields, field);
        }
        Schema schema = schemaFields.endRecord();
        return schema;
    }

    private static SchemaBuilder.FieldAssembler addSchemaField(
            SchemaBuilder.FieldAssembler schemaField, Type.StructField field) {

        switch(field.getType().getCode()) {
            case STRING:
                return schemaField.optionalString(field.getName());
            case BYTES:
                return schemaField.optionalBytes(field.getName());
            case INT64:
                return schemaField.optionalLong(field.getName());
            case FLOAT64:
                return schemaField.optionalDouble(field.getName());
            case BOOL:
                return schemaField.optionalBoolean(field.getName());
            case TIMESTAMP:
                return schemaField.name(field.getName()).type(LOGICAL_TIMESTAMP_TYPE).withDefault(null);
            case DATE:
                return schemaField.name(field.getName()).type(LOGICAL_DATE_TYPE).withDefault(null);
            case STRUCT:
                SchemaBuilder.FieldAssembler<SchemaBuilder.FieldAssembler<Schema>> structField =
                        schemaField.name(field.getName()).type().optional().record(field.getName()).fields();
                for(final Type.StructField childField : field.getType().getStructFields()) {
                    structField = addSchemaField(structField, childField);
                }
                return structField.endRecord();
            case ARRAY:
                if(Type.Code.STRUCT.equals(field.getType().getArrayElementType().getCode())) {
                    SchemaBuilder.FieldAssembler<SchemaBuilder.FieldAssembler<Schema>> arrayField =
                            schemaField.name(field.getName()).type().optional().array().items().record(field.getName()).fields();
                    for(final Type.StructField childField : field.getType().getArrayElementType().getStructFields()) {
                        arrayField = addSchemaField(arrayField, childField);
                    }
                    return arrayField.endRecord();
                }
                Schema arrayElementSchema = convertType(field.getType().getArrayElementType());
                Schema arraySchema = Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(arrayElementSchema));
                return schemaField.name(field.getName()).type(arraySchema).withDefault(null);
            default:
                return schemaField;
        }
    }

    private static Schema convertType(Type type) {
        switch(type.getCode()) {
            case STRING:
                return NULLABLE_STRING;
            case BYTES:
                return NULLABLE_BYTES;
            case INT64:
                return NULLABLE_LONG;
            case FLOAT64:
                return NULLABLE_DOUBLE;
            case BOOL:
                return NULLABLE_BOOLEAN;
            case TIMESTAMP:
                return LOGICAL_TIMESTAMP_TYPE;
            case DATE:
                return LOGICAL_DATE_TYPE;
            case STRUCT:
                return Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.RECORD));
            case ARRAY:
                return Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.ARRAY));
            default:
                return Schema.create(Schema.Type.NULL);
        }
    }

    public static GenericRecord convertGenericRecord(Struct struct, Schema schema) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            setFieldValue(builder, field, struct);
        }
        return builder.build();
    }

    private static void setFieldValue(GenericRecordBuilder builder, Schema.Field field, Struct struct) {
        setFieldValue(builder, field.schema(), field, struct);
    }

    private static void setFieldValue(GenericRecordBuilder builder, Schema schema, Schema.Field field, Struct struct) {
        final Optional<Type> type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(field.name()))
                .map(f -> f.getType())
                .findFirst();

        if(!type.isPresent()) {
            throw new IllegalArgumentException(String.format("Missing field %s", field.name()));
        }

        if(struct.isNull(field.name())) {
            builder.set(field.name(), null);
            return;
        }

        switch (schema.getType()) {
            case STRING:
                builder.set(field, struct.getString(field.name()));
                break;
            case BYTES:
                builder.set(field, struct.getBytes(field.name()));
                break;
            case ENUM:
                builder.set(field, struct.getString(field.name()));
                break;
            case INT:
                if(Type.date().equals(type.get())) {
                    final Date date = struct.getDate(field.name());
                    final DateTime datetime = new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
                    final Days days = Days.daysBetween(EPOCH_DATETIME, datetime);
                    builder.set(field, days.getDays());
                } else {
                    builder.set(field, struct.getLong(field.name()));
                }
                break;
            case LONG:
                if(Type.timestamp().equals(type.get())) {
                    builder.set(field, struct.getTimestamp(field.name()).getSeconds() * 1000);
                } else {
                    builder.set(field, struct.getLong(field.name()));
                }
                break;
            case FLOAT:
                builder.set(field, struct.getDouble(field.name()));
                break;
            case DOUBLE:
                builder.set(field, struct.getDouble(field.name()));
                break;
            case BOOLEAN:
                builder.set(field, struct.getBoolean(field.name()));
                break;
            case FIXED:
                builder.set(field, struct.getString(field.name()));
                break;
            case RECORD:
                final GenericRecord chileRecord = convertGenericRecord(struct.getStruct(field.name()), schema);
                builder.set(field, chileRecord);
                break;
            case MAP:
                builder.set(field, struct.getStruct(field.name()));
                break;
            case UNION:
                for(final Schema childSchema : field.schema().getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    setFieldValue(builder, childSchema, field, struct);
                }
                break;
            case ARRAY:
                setArrayFieldValue(builder, schema.getElementType(), field, struct);
                break;
            case NULL:
                builder.set(field.name(), null);
                break;
            default:
                break;
        }
    }

    private static void setArrayFieldValue(GenericRecordBuilder builder, Schema schema, Schema.Field field, Struct struct) {
        final Optional<Type> type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(field.name()))
                .map(f -> f.getType())
                .findFirst();

        if(!type.isPresent()) {
            throw new IllegalArgumentException(String.format("Missing field %s", field.name()));
        }
        if(struct.isNull(field.name())) {
            builder.set(field.name(), null);
            return;
        }
        switch (schema.getType()) {
            case STRING:
                builder.set(field, struct.getStringList(field.name()));
                break;
            case BYTES:
                builder.set(field, struct.getBytesList(field.name()));
                break;
            case ENUM:
                builder.set(field, struct.getStringList(field.name()));
                break;
            case INT:
                final List<Integer> dateList = new ArrayList<>();
                if(Type.array(Type.date()).equals(type.get())) {
                    for (final Date date : struct.getDateList(field.name())) {
                        final DateTime datetime = new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
                        final Days days = Days.daysBetween(EPOCH_DATETIME, datetime);
                        dateList.add(days.getDays());
                    }
                    builder.set(field, dateList);
                } else {
                    builder.set(field, struct.getLongList(field.name()));
                }
                break;
            case LONG:
                if(Type.array(Type.timestamp()).equals(type.get())) {
                    final List<Long> timestampList = new ArrayList<>();
                    for(final Timestamp timestamp : struct.getTimestampList(field.name())) {
                        timestampList.add(timestamp.getSeconds() * 1000);
                    }
                    builder.set(field, timestampList);
                } else {
                    builder.set(field, struct.getLongList(field.name()));
                }
                break;
            case FLOAT:
                builder.set(field, struct.getDoubleList(field.name()));
                break;
            case DOUBLE:
                builder.set(field, struct.getDoubleList(field.name()));
                break;
            case BOOLEAN:
                builder.set(field, struct.getBooleanList(field.name()));
                break;
            case FIXED:
                builder.set(field, struct.getStringList(field.name()));
                break;
            case RECORD:
                final List<GenericRecord> childRecords = new ArrayList<>();
                for(final Struct childStruct : struct.getStructList(field.name())) {
                    final GenericRecord childRecord = convertGenericRecord(childStruct, schema);
                    childRecords.add(childRecord);
                }
                builder.set(field, childRecords);
                break;
            case MAP:
                builder.set(field, struct.getStructList(field.name()));
                break;
            case UNION:
                for(final Schema childSchema : field.schema().getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    for(final Schema cchildSchema : childSchema.getElementType().getTypes()) {
                        if(Schema.Type.NULL.equals(cchildSchema.getType())) {
                            continue;
                        }
                        setArrayFieldValue(builder, cchildSchema, field, struct);
                    }
                }
                break;
            case ARRAY:
                // IMPOSSIBLE ARRAY IN ARRAY ??
                break;
            case NULL:
                builder.set(field.name(), null);
                break;
            default:
                break;
        }
    }

    public static Struct convertStruct(GenericRecord record) {
        Struct.Builder builder = Struct.newBuilder();
        for(final Schema.Field field : record.getSchema().getFields()) {
            builder = setFieldValue(builder, field, field.schema(), record);
        }
        return builder.build();
    }

    private static Struct.Builder setFieldValue(Struct.Builder builder, Schema.Field field, Schema type, GenericRecord record) {
        switch (type.getType()) {
            case STRING:
                return builder.set(field.name()).to(record.get(field.name()) == null ? null : record.get(field.name()).toString());
            case BYTES:
                return builder.set(field.name()).to((ByteArray) record.get(field.name()));
            case ENUM:
                return builder.set(field.name()).to(record.get(field.name()) == null ? null : record.get(field.name()).toString());
            case INT:
                final Long intvalue;
                if(record.get(field.name()) == null) {
                    intvalue = null;
                } else {
                    intvalue = new Long((Integer)record.get(field.name()));
                }
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
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())) {
                    if(longvalue == null) {
                        return builder.set(field.name()).to((Timestamp)null);
                    }
                    return builder.set(field.name()).to(Timestamp.ofTimeMicroseconds(longvalue * 1000));
                }
                return builder.set(field.name()).to(longvalue);
            case FLOAT:
                return builder.set(field.name()).to((Double) record.get(field.name()));
            case DOUBLE:
                return builder.set(field.name()).to((Double)record.get(field.name()));
            case BOOLEAN:
                return builder.set(field.name()).to((Boolean)record.get(field.name()));
            case FIXED:
                return builder.set(field.name()).to((String)record.get(field.name()));
            case RECORD:
                final Struct childStruct = convertStruct((GenericRecord)record.get(field.name()));
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
                return builder;
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
                return builder.set(field.name()).toBytesArray((List<ByteArray>) record.get(field.name()));
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
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())) {
                    if(longvalues == null) {
                        return builder.set(field.name()).toTimestampArray(null);
                    }
                    final List<Timestamp> timestampList = new ArrayList<>();
                    for(final Long epocmills : longvalues) {
                        if(epocmills == null) {
                            timestampList.add(null);
                            continue;
                        }
                        final Timestamp timestamp = Timestamp.ofTimeMicroseconds(epocmills * 1000);
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
                //return builder.set(field.name()).toStructArray(structList);
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
}
