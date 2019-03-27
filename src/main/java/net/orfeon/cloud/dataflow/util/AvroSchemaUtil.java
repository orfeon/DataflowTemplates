package net.orfeon.cloud.dataflow.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;
import java.util.stream.Collectors;

public class AvroSchemaUtil {

    private enum TableRowFieldType {
        STRING,
        BYTES,
        INT64,
        INTEGER,
        FLOAT64,
        FLOAT,
        NUMERIC,
        BOOLEAN,
        BOOL,
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        GEOGRAPHY,
        STRUCT,
        RECORD
    }

    private enum TableRowFieldMode {
        REQUIRED,
        NULLABLE,
        REPEATED
    }

    private static final Schema REQUIRED_STRING = Schema.create(Schema.Type.STRING);
    private static final Schema REQUIRED_BYTES = Schema.create(Schema.Type.BYTES);
    private static final Schema REQUIRED_BOOLEAN = Schema.create(Schema.Type.BOOLEAN);
    private static final Schema REQUIRED_LONG = Schema.create(Schema.Type.LONG);
    private static final Schema REQUIRED_DOUBLE = Schema.create(Schema.Type.DOUBLE);

    private static final Schema REQUIRED_LOGICAL_DATE_TYPE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    private static final Schema REQUIRED_LOGICAL_TIME_MICRO_TYPE = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    private static final Schema REQUIRED_LOGICAL_TIMESTAMP_TYPE = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    private static final Schema REQUIRED_LOGICAL_DECIMAL_TYPE = LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES));

    private static final Schema NULLABLE_STRING = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    private static final Schema NULLABLE_BYTES = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES));
    private static final Schema NULLABLE_BOOLEAN = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN));
    private static final Schema NULLABLE_LONG = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
    private static final Schema NULLABLE_DOUBLE = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE));

    private static final Schema NULLABLE_LOGICAL_DATE_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
    private static final Schema NULLABLE_LOGICAL_TIME_MILLI_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)));
    private static final Schema NULLABLE_LOGICAL_TIME_MICRO_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    private static final Schema NULLABLE_LOGICAL_TIMESTAMP_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));
    private static final Schema NULLABLE_LOGICAL_DECIMAL_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES)));


    public static Schema convertSchema(TableSchema tableSchema) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(final TableFieldSchema fieldSchema : tableSchema.getFields()) {
            schemaFields.name(fieldSchema.getName()).type(convertSchema(fieldSchema)).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static Schema convertSchema(com.google.cloud.bigquery.Schema tableSchema) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(final Field field : tableSchema.getFields()) {
            schemaFields.name(field.getName()).type(convertSchema(field, field.getMode() == null ? Field.Mode.NULLABLE : field.getMode(), "root")).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static Schema convertSchema(Struct struct) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(final Type.StructField structField : struct.getType().getStructFields()) {
            schemaFields.name(structField.getName()).type(convertSchema(structField.getName(), structField.getType())).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static boolean isLogicalTypeDecimal(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isLogicalTypeDecimal(childSchema);
        }
        final int precision = schema.getObjectProp("precision") != null ?
                Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
        final int scale = schema.getObjectProp("scale") != null ?
                Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
        return LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType());
    }

    public static boolean isSqlTypeDatetime(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isSqlTypeDatetime(childSchema);
        }
        final String sqlType = schema.getProp("sqlType");
        return "DATETIME".equals(sqlType);
    }

    public static boolean isSqlTypeGeography(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isSqlTypeGeography(childSchema);
        }
        final String sqlType = schema.getProp("sqlType");
        return "GEOGRAPHY".equals(sqlType);
    }

    public static LogicalTypes.Decimal getLogicalTypeDecimal(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return getLogicalTypeDecimal(childSchema);
        }
        final int precision = schema.getObjectProp("precision") != null ?
                Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
        final int scale = schema.getObjectProp("scale") != null ?
                Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
        return LogicalTypes.decimal(precision, scale);
    }

    private static Schema unnestUnion(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            return schema.getTypes().stream()
                    .filter(s -> !s.getType().equals(Schema.Type.NULL))
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
        }
        return schema;
    }

    private static Schema convertSchema(final TableFieldSchema fieldSchema) {
        return convertSchema(fieldSchema, TableRowFieldMode.valueOf(fieldSchema.getMode() == null ? TableRowFieldMode.NULLABLE.name() : fieldSchema.getMode()));
    }

    private static Schema convertSchema(final TableFieldSchema fieldSchema, TableRowFieldMode mode) {
        if(mode.equals(TableRowFieldMode.REPEATED)) {
            return Schema.createUnion(
                    Schema.create(Schema.Type.NULL),
                    Schema.createArray(convertSchema(fieldSchema, TableRowFieldMode.NULLABLE)));
        }
        switch(TableRowFieldType.valueOf(fieldSchema.getType())) {
            case DATETIME:
                final Schema datetimeSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
                datetimeSchema.addProp("sqlType", "DATETIME");
                return datetimeSchema;
            case GEOGRAPHY:
                final Schema geoSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
                geoSchema.addProp("sqlType", "GEOGRAPHY");
                return geoSchema;
            case STRING: return NULLABLE_STRING;
            case BYTES: return NULLABLE_BYTES;
            case INT64:
            case INTEGER: return NULLABLE_LONG;
            case FLOAT64:
            case FLOAT: return NULLABLE_DOUBLE;
            case BOOL:
            case BOOLEAN: return NULLABLE_BOOLEAN;
            case DATE: return NULLABLE_LOGICAL_DATE_TYPE;
            case TIME: return NULLABLE_LOGICAL_TIME_MICRO_TYPE;
            case TIMESTAMP: return NULLABLE_LOGICAL_TIMESTAMP_TYPE;
            case NUMERIC: return NULLABLE_LOGICAL_DECIMAL_TYPE;
            case STRUCT:
            case RECORD:
                final List<Schema.Field> fields = fieldSchema.getFields().stream()
                        .map(f -> new Schema.Field(f.getName(), convertSchema(f, TableRowFieldMode.valueOf(f.getMode())), null, (Object)null, Schema.Field.Order.IGNORE))
                        .collect(Collectors.toList());
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createRecord(fieldSchema.getName(), fieldSchema.getDescription(), null, false, fields));
            default: throw new IllegalArgumentException();
        }
    }

    private static Schema convertSchema(final Field field, Field.Mode mode, String namespace) {
        if(mode.equals(Field.Mode.REPEATED)) {
            return Schema.createArray(convertSchema(field, Field.Mode.REQUIRED, namespace));
        }
        switch(field.getType().getStandardType()) {
            case DATETIME:
                final Schema datetimeSchema = Field.Mode.REQUIRED.equals(mode) ?
                        Schema.create(Schema.Type.STRING) :
                        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
                datetimeSchema.addProp("sqlType", "DATETIME");
                return datetimeSchema;
            case GEOGRAPHY:
                final Schema geoSchema = Field.Mode.REQUIRED.equals(mode) ?
                        Schema.create(Schema.Type.STRING) :
                        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
                geoSchema.addProp("sqlType", "GEOGRAPHY");
                return geoSchema;
            case STRING: return Field.Mode.REQUIRED.equals(mode) ? REQUIRED_STRING : NULLABLE_STRING;
            case BYTES: return Field.Mode.REQUIRED.equals(mode) ? REQUIRED_BYTES : NULLABLE_BYTES;
            case INT64: return Field.Mode.REQUIRED.equals(mode) ? REQUIRED_LONG : NULLABLE_LONG;
            case FLOAT64: return Field.Mode.REQUIRED.equals(mode) ? REQUIRED_DOUBLE : NULLABLE_DOUBLE;
            case BOOL: return Field.Mode.REQUIRED.equals(mode) ? REQUIRED_BOOLEAN : NULLABLE_BOOLEAN;
            case DATE: return Field.Mode.REQUIRED.equals(mode) ? REQUIRED_LOGICAL_DATE_TYPE : NULLABLE_LOGICAL_DATE_TYPE;
            case TIME: return Field.Mode.REQUIRED.equals(mode) ? REQUIRED_LOGICAL_TIME_MICRO_TYPE : NULLABLE_LOGICAL_TIME_MICRO_TYPE;
            case TIMESTAMP: return Field.Mode.REQUIRED.equals(mode) ? REQUIRED_LOGICAL_TIMESTAMP_TYPE : NULLABLE_LOGICAL_TIMESTAMP_TYPE;
            case NUMERIC: return Field.Mode.REQUIRED.equals(mode) ? REQUIRED_LOGICAL_DECIMAL_TYPE : NULLABLE_LOGICAL_DECIMAL_TYPE;
            case STRUCT:
                final List<Schema.Field> fields = field.getSubFields().stream()
                        .map(subField -> new Schema.Field(
                                subField.getName(),
                                convertSchema(subField, subField.getMode(),  namespace + "." + field.getName()),
                                subField.getDescription(),
                                (Object)null,
                                Schema.Field.Order.IGNORE))
                        .collect(Collectors.toList());
                if(Field.Mode.REQUIRED.equals(mode)) {
                    return Schema.createRecord(field.getName(), field.getDescription(), namespace + "." + field.getName(), false, fields);
                } else {
                    return Schema.createUnion(
                            Schema.create(Schema.Type.NULL),
                            Schema.createRecord(field.getName(), field.getDescription(), namespace + "." + field.getName(), false, fields));
                }
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
            case DATE: return NULLABLE_LOGICAL_DATE_TYPE;
            case TIMESTAMP: return NULLABLE_LOGICAL_TIMESTAMP_TYPE;
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
