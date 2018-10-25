package net.orfeon.cloud.dataflow.storage;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AvroUtil {

    private AvroUtil() {

    }

    public static Schema convertSchemaFromStruct(Struct struct) {
        SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(Type.StructField field : struct.getType().getStructFields()) {
            schemaFields = addSchemaField(schemaFields, field);
        }
        return schemaFields.endRecord();
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
                return schemaField.optionalLong(field.getName());
            case DATE:
                return schemaField.optionalString(field.getName());
            case STRUCT:
                SchemaBuilder.FieldAssembler<SchemaBuilder.FieldAssembler<Schema>> structField =
                        schemaField.name(field.getName()).type().optional().record(field.getName()).fields();
                for(final Type.StructField childField : field.getType().getStructFields()) {
                    structField = addSchemaField(structField, childField);
                }
                return structField.endRecord();
            case ARRAY:
                final Schema.Type avroType = convertType(field.getType().getArrayElementType());
                if(Schema.Type.RECORD.equals(avroType)) {
                    SchemaBuilder.FieldAssembler<SchemaBuilder.FieldAssembler<Schema>> arrayField =
                            schemaField.name(field.getName()).type().optional().array().items().record(field.getName()).fields();
                    for(final Type.StructField childField : field.getType().getArrayElementType().getStructFields()) {
                        arrayField = addSchemaField(arrayField, childField);
                    }
                    return arrayField.endRecord();
                }
                return schemaField.name(field.getName()).type(SchemaBuilder.array().items(Schema.create(avroType))).noDefault();
            default:
                return schemaField;
        }
    }

    private static Schema.Type convertType(Type type) {
        switch(type.getCode()) {
            case STRING:
                return Schema.Type.STRING;
            case BYTES:
                return Schema.Type.BYTES;
            case INT64:
                return Schema.Type.LONG;
            case FLOAT64:
                return Schema.Type.DOUBLE;
            case BOOL:
                return Schema.Type.BOOLEAN;
            case TIMESTAMP:
                return Schema.Type.LONG;
            case DATE:
                return Schema.Type.STRING;
            case STRUCT:
                return Schema.Type.RECORD;
            case ARRAY:
                return Schema.Type.ARRAY;
            default:
                return Schema.Type.NULL;
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
                if(Type.date().equals(type.get())) {
                    builder.set(field, struct.getDate(field.name()).toString());
                } else {
                    builder.set(field, struct.getString(field.name()));
                }
                break;
            case BYTES:
                builder.set(field, struct.getBytes(field.name()));
                break;
            case ENUM:
                builder.set(field, struct.getString(field.name()));
                break;
            case INT:
                builder.set(field, struct.getLong(field.name()));
                break;
            case LONG:
                if(Type.timestamp().equals(type.get())) {
                    builder.set(field, struct.getTimestamp(field.name()).getSeconds());
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
                    if(childSchema.getType().equals(Schema.Type.NULL)) {
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
                if(Type.array(Type.date()).equals(type.get())) {
                    final List<String> dateList = new ArrayList<>();
                    for(final Date date : struct.getDateList(field.name())) {
                        dateList.add(date.toString());
                    }
                    builder.set(field, dateList);
                } else {
                    builder.set(field, struct.getStringList(field.name()));
                }
                break;
            case BYTES:
                builder.set(field, struct.getBytesList(field.name()));
                break;
            case ENUM:
                builder.set(field, struct.getStringList(field.name()));
                break;
            case INT:
                builder.set(field, struct.getLongList(field.name()));
                break;
            case LONG:
                if(Type.array(Type.timestamp()).equals(type.get())) {
                    final List<Long> timestampList = new ArrayList<>();
                    for(final Timestamp timestamp : struct.getTimestampList(field.name())) {
                        timestampList.add(timestamp.getSeconds());
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
                    if(childSchema.getType().equals(Schema.Type.NULL)) {
                        continue;
                    }
                    setArrayFieldValue(builder, childSchema, field, struct);
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
