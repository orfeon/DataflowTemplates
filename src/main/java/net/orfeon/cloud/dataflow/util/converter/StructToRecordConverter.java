package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class StructToRecordConverter {

    private static final MutableDateTime EPOCH_DATETIME = new MutableDateTime(0, DateTimeZone.UTC);
    private static final Schema NULLABLE_STRING = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    private static final Schema NULLABLE_BYTES = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES));
    private static final Schema NULLABLE_BOOLEAN = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN));
    private static final Schema NULLABLE_LONG = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
    private static final Schema NULLABLE_DOUBLE = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE));
    private static final Schema LOGICAL_DATE_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
    private static final Schema LOGICAL_TIMESTAMP_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));

    private StructToRecordConverter() {

    }

    public static GenericRecord convert(Struct struct, Schema schema) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            setFieldValue(builder, field, struct);
        }
        return builder.build();
    }

    public static Schema convertSchema(Struct struct) {
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
                final GenericRecord chileRecord = convert(struct.getStruct(field.name()), schema);
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
                    final GenericRecord childRecord = convert(childStruct, schema);
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

}
