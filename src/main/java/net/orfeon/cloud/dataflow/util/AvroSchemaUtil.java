package net.orfeon.cloud.dataflow.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class AvroSchemaUtil {

    private enum TableRowFieldType {
        STRING,
        BYTES,
        INT64,
        FLOAT64,
        NUMERIC,
        BOOL,
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        GEOGRAPHY,
        STRUCT
    }

    private enum TableRowFieldMode {
        REQUIRED,
        NULLABLE,
        REPEATED
    }

    private static final Schema NULLABLE_STRING = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    private static final Schema NULLABLE_BYTES = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES));
    private static final Schema NULLABLE_BOOLEAN = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN));
    private static final Schema NULLABLE_LONG = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
    private static final Schema NULLABLE_DOUBLE = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE));

    private static final Schema LOGICAL_DATE_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
    private static final Schema LOGICAL_TIME_MILLI_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)));
    private static final Schema LOGICAL_TIME_MICRO_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    private static final Schema LOGICAL_TIMESTAMP_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));
    private static final Schema LOGICAL_DECIMAL_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES)));


    public static Schema convertSchema(TableSchema tableSchema) {
        SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(final TableFieldSchema fieldSchema : tableSchema.getFields()) {
            schemaFields = schemaFields.name(fieldSchema.getName()).type(convertSchema(fieldSchema)).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static Schema convertSchema(Struct struct) {
        SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(final Type.StructField structField : struct.getType().getStructFields()) {
            schemaFields = schemaFields.name(structField.getName()).type(convertSchema(structField.getName(), structField.getType())).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static boolean isLogicalTypeDecimal(Schema schema) {
        final int precision = schema.getObjectProp("precision") != null ?
                Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
        final int scale = schema.getObjectProp("scale") != null ?
                Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
        return LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType());
    }

    public static LogicalTypes.Decimal getLogicalTypeDecimal(Schema schema) {
        final int precision = schema.getObjectProp("precision") != null ?
                Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
        final int scale = schema.getObjectProp("scale") != null ?
                Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
        return LogicalTypes.decimal(precision, scale);
    }

    private static Schema convertSchema(final TableFieldSchema fieldSchema) {
        return convertSchema(fieldSchema, TableRowFieldMode.NULLABLE);
    }

    private static Schema convertSchema(final TableFieldSchema fieldSchema, TableRowFieldMode mode) {
        if(mode.equals(TableRowFieldMode.REPEATED)) {
            return Schema.createUnion(
                    Schema.create(Schema.Type.NULL),
                    Schema.createArray(convertSchema(fieldSchema)));
        }
        switch(TableRowFieldType.valueOf(fieldSchema.getType())) {
            case DATETIME:
            case STRING: return NULLABLE_STRING;
            case BYTES: return NULLABLE_BYTES;
            case INT64: return NULLABLE_LONG;
            case FLOAT64: return NULLABLE_DOUBLE;
            case BOOL: return NULLABLE_BOOLEAN;
            case DATE: return LOGICAL_DATE_TYPE;
            case TIME: return LOGICAL_TIME_MILLI_TYPE;
            case TIMESTAMP: return LOGICAL_TIMESTAMP_TYPE;
            case NUMERIC: return LOGICAL_DECIMAL_TYPE;
            case STRUCT:
                final List<Schema.Field> fields = fieldSchema.getFields().stream()
                        .map(s -> new Schema.Field(s.getName(), convertSchema(s, TableRowFieldMode.valueOf(s.getMode())), null, (Object)null, Schema.Field.Order.IGNORE))
                        .collect(Collectors.toList());
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createRecord(fields));
            default: throw new IllegalArgumentException();
        }
    }

    private static Schema convertSchema(final String name, final Type structFieldType) {
        switch(structFieldType.getCode()) {
            case STRING: return NULLABLE_STRING;
            case BYTES: return NULLABLE_BYTES;
            case INT64: return NULLABLE_LONG;
            case FLOAT64: return NULLABLE_DOUBLE;
            case BOOL: return NULLABLE_BOOLEAN;
            case DATE: return LOGICAL_DATE_TYPE;
            case TIMESTAMP: return LOGICAL_TIMESTAMP_TYPE;
            case STRUCT:
                final List<Schema.Field> fields = structFieldType.getStructFields().stream()
                        .map(s -> new Schema.Field(s.getName(), convertSchema(s.getName(), s.getType()), null, (Object)null, Schema.Field.Order.IGNORE))
                        .collect(Collectors.toList());
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createRecord(name, null, null, false, fields));
            case ARRAY:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(convertSchema(name, structFieldType.getArrayElementType())));
            default:
                throw new IllegalArgumentException();

        }
    }

}
