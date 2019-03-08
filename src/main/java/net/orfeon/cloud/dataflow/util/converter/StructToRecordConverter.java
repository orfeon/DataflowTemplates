package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.Date;
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

import java.util.Optional;
import java.util.stream.Collectors;


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
        for(final Type.StructField field : struct.getType().getStructFields()) {
            schemaFields = addSchemaField(schemaFields, field);
        }
        return schemaFields.endRecord();
    }

    private static SchemaBuilder.FieldAssembler addSchemaField(
            SchemaBuilder.FieldAssembler schemaField, Type.StructField field) {

        final String fieldName = field.getName();
        switch(field.getType().getCode()) {
            case STRING:
                return schemaField.optionalString(fieldName);
            case BYTES:
                return schemaField.optionalBytes(fieldName);
            case INT64:
                return schemaField.optionalLong(fieldName);
            case FLOAT64:
                return schemaField.optionalDouble(fieldName);
            case BOOL:
                return schemaField.optionalBoolean(fieldName);
            case TIMESTAMP:
                return schemaField.name(fieldName).type(LOGICAL_TIMESTAMP_TYPE).withDefault(null);
            case DATE:
                return schemaField.name(fieldName).type(LOGICAL_DATE_TYPE).withDefault(null);
            case STRUCT:
                SchemaBuilder.FieldAssembler<SchemaBuilder.FieldAssembler<Schema>> structField =
                        schemaField.name(fieldName).type().optional().record(fieldName).fields();
                for(final Type.StructField childField : field.getType().getStructFields()) {
                    structField = addSchemaField(structField, childField);
                }
                return structField.endRecord();
            case ARRAY:
                if(Type.Code.STRUCT.equals(field.getType().getArrayElementType().getCode())) {
                    SchemaBuilder.FieldAssembler<SchemaBuilder.FieldAssembler<Schema>> arrayField = schemaField
                            .name(fieldName)
                            .type()
                            .optional()
                            .array()
                            .items()
                            .record(fieldName)
                            .fields();
                    for(final Type.StructField childField : field.getType().getArrayElementType().getStructFields()) {
                        arrayField = addSchemaField(arrayField, childField);
                    }
                    return arrayField.endRecord();
                }
                final Schema arrayElementSchema = convertTypeToAvroSchema(field.getType().getArrayElementType());
                final Schema arraySchema = Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(arrayElementSchema));
                return schemaField.name(fieldName).type(arraySchema).withDefault(null);
            default:
                return schemaField;
        }
    }

    private static Schema convertTypeToAvroSchema(Type type) {
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
        setFieldValue(builder, field, field.schema(), struct);
    }

    private static void setFieldValue(GenericRecordBuilder builder, Schema.Field field, Schema schema, Struct struct) {
        final String fieldName = field.name();
        final Optional<Type> type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .map(f -> f.getType())
                .findFirst();

        if(!type.isPresent()) {
            throw new IllegalArgumentException(String.format("Missing field %s", fieldName));
        }

        if(struct.isNull(fieldName)) {
            builder.set(fieldName, null);
            return;
        }

        switch (schema.getType()) {
            case STRING:
                builder.set(field, struct.getString(fieldName));
                break;
            case BYTES:
                builder.set(field, struct.getBytes(fieldName).asReadOnlyByteBuffer());
                break;
            case INT:
                if(Type.date().equals(type.get())) {
                    final Date date = struct.getDate(fieldName);
                    final DateTime datetime = new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
                    final Days days = Days.daysBetween(EPOCH_DATETIME, datetime);
                    builder.set(field, days.getDays());
                } else {
                    builder.set(field, struct.getLong(fieldName));
                }
                break;
            case LONG:
                if(Type.timestamp().equals(type.get())) {
                    builder.set(field, struct.getTimestamp(fieldName).getSeconds() * 1000);
                } else {
                    builder.set(field, struct.getLong(fieldName));
                }
                break;
            case DOUBLE:
                builder.set(field, struct.getDouble(fieldName));
                break;
            case BOOLEAN:
                builder.set(field, struct.getBoolean(fieldName));
                break;
            case RECORD:
                final GenericRecord chileRecord = convert(struct.getStruct(fieldName), schema);
                builder.set(field, chileRecord);
                break;
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    setFieldValue(builder, field, childSchema, struct);
                }
                break;
            case ARRAY:
                setArrayFieldValue(builder, field, schema.getElementType(), struct);
                break;
            case NULL:
                builder.set(fieldName, null);
                break;
            case FLOAT:
            case ENUM:
            case MAP:
            case FIXED:
                break;
            default:
                break;
        }
    }

    private static void setArrayFieldValue(GenericRecordBuilder builder, Schema.Field field, Schema schema, Struct struct) {
        final String fieldName = field.name();
        final Optional<Type> type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .map(f -> f.getType())
                .findFirst();

        if(!type.isPresent()) {
            throw new IllegalArgumentException(String.format("Missing field %s", fieldName));
        }
        if(struct.isNull(fieldName)) {
            builder.set(fieldName, null);
            return;
        }
        switch (schema.getType()) {
            case STRING:
                builder.set(field, struct.getStringList(fieldName));
                break;
            case BYTES:
                builder.set(field, struct.getBytesList(fieldName)
                        .stream()
                        .map(b -> b.asReadOnlyByteBuffer())
                        .collect(Collectors.toList()));
                break;
            case INT:
                if(Type.array(Type.date()).equals(type.get())) {
                    builder.set(field, struct.getDateList(fieldName).stream()
                            .map(date -> new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC))
                            .map(datetime -> Days.daysBetween(EPOCH_DATETIME, datetime).getDays())
                            .collect(Collectors.toList()));
                } else {
                    builder.set(field, struct.getLongList(fieldName));
                }
                break;
            case LONG:
                if(Type.array(Type.timestamp()).equals(type.get())) {
                    builder.set(field, struct.getTimestampList(fieldName).stream()
                            .map(timestamp -> timestamp.getSeconds() * 1000)
                            .collect(Collectors.toList()));
                } else {
                    builder.set(field, struct.getLongList(fieldName));
                }
                break;
            case DOUBLE:
                builder.set(field, struct.getDoubleList(fieldName));
                break;
            case BOOLEAN:
                builder.set(field, struct.getBooleanList(fieldName));
                break;
            case RECORD:
                builder.set(field, struct.getStructList(fieldName).stream()
                        .map(childStruct -> convert(childStruct, schema))
                        .collect(Collectors.toList()));
                break;
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    setArrayFieldValue(builder, field, childSchema, struct);
                }
                break;
            case ARRAY:
                setArrayFieldValue(builder, field, schema.getElementType(), struct);
                break;
            case NULL:
                builder.set(fieldName, null);
                break;
            case FLOAT:
            case ENUM:
            case MAP:
            case FIXED:
                break;
            default:
                break;
        }
    }

}
