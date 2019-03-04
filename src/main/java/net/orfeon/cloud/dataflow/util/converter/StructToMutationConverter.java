package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.spanner.v1.KeySet;

public class StructToMutationConverter {

    private StructToMutationConverter() {

    }

    public static Mutation convert(final Struct struct, final String table, final Mutation.Op mutationOp) {
        Mutation.WriteBuilder builder = createMutationWriteBuilder(table, mutationOp);
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final String fieldName = field.getName();
            final boolean isNullField = struct.isNull(fieldName);
            switch(field.getType().getCode()) {
                case STRING:
                    builder = builder.set(field.getName()).to(isNullField ? null : struct.getString(fieldName));
                    break;
                case BYTES:
                    builder = builder.set(field.getName()).to(isNullField ? null : struct.getBytes(fieldName));
                    break;
                case BOOL:
                    builder = builder.set(field.getName()).to(isNullField ? null : struct.getBoolean(fieldName));
                    break;
                case INT64:
                    builder = builder.set(field.getName()).to(isNullField ? null : struct.getLong(fieldName));
                    break;
                case FLOAT64:
                    builder = builder.set(field.getName()).to(isNullField ? null : struct.getDouble(fieldName));
                    break;
                case DATE:
                    builder = builder.set(field.getName()).to(isNullField ? null : struct.getDate(fieldName));
                    break;
                case TIMESTAMP:
                    builder = builder.set(field.getName()).to(isNullField ? null : struct.getTimestamp(fieldName));
                    break;
                case STRUCT:
                    builder = builder.set(field.getName()).to(isNullField ? null : struct.getStruct(fieldName));
                    break;
                case ARRAY:
                    switch (field.getType().getArrayElementType().getCode()) {
                        case STRING:
                            builder = builder.set(field.getName()).toStringArray(isNullField ? null : struct.getStringList(fieldName));
                            break;
                        case BYTES:
                            builder = builder.set(field.getName()).toBytesArray(isNullField ? null : struct.getBytesList(fieldName));
                            break;
                        case BOOL:
                            builder = builder.set(field.getName()).toBoolArray(isNullField ? null : struct.getBooleanArray(fieldName));
                            break;
                        case INT64:
                            builder = builder.set(field.getName()).toInt64Array(isNullField ? null : struct.getLongArray(fieldName));
                            break;
                        case FLOAT64:
                            builder = builder.set(field.getName()).toFloat64Array(isNullField ? null : struct.getDoubleArray(fieldName));
                            break;
                        case DATE:
                            builder = builder.set(field.getName()).toDateArray(isNullField ? null : struct.getDateList(fieldName));
                            break;
                        case TIMESTAMP:
                            builder = builder.set(field.getName()).toTimestampArray(isNullField ? null : struct.getTimestampList(fieldName));
                            break;
                        case STRUCT:
                            // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2018/10/26)
                            //builder = binder.toStructArray(isNullField ? null : struct.getStructList(fieldName));
                            break;
                        case ARRAY:
                            // NOT SUPPOERTED TO STORE ARRAY IN ARRAY FIELD! (2018/10/26)
                            break;
                    }

            }
        }
        return builder.build();
    }

    public static Mutation delete(final Struct struct, final String table, final String keyFields) {
        Key.Builder builder = Key.newBuilder();
        for(final String keyField : keyFields.split(",")) {
            if(struct.isNull(keyField)) {
                throw new IllegalArgumentException(String.format("KeyField: %s must not be null!", keyField));
            }
            switch(struct.getColumnType(keyField).getCode()) {
                case STRING:
                    builder = builder.append(struct.getString(keyField));
                    break;
                case BYTES:
                    builder = builder.append(struct.getBytes(keyField));
                    break;
                case BOOL:
                    builder = builder.append(struct.getBoolean(keyField));
                    break;
                case INT64:
                    builder = builder.append(struct.getLong(keyField));
                    break;
                case FLOAT64:
                    builder = builder.append(struct.getDouble(keyField));
                    break;
                case DATE:
                    builder = builder.append(struct.getDate(keyField));
                    break;
                case TIMESTAMP:
                    builder = builder.append(struct.getTimestamp(keyField));
                    break;
                default:
                    throw new IllegalArgumentException(String.format(
                            "field: %s, fieldType: %s at table %s, is impossible as Key.",
                            keyField, struct.getColumnType(keyField).toString(), table));
            }
        }
        return Mutation.delete(table, builder.build());
    }

    private static Mutation.WriteBuilder createMutationWriteBuilder(final String table, final Mutation.Op mutationOp) {
        switch(mutationOp) {
            case INSERT:
                return Mutation.newInsertBuilder(table);
            case UPDATE:
                return Mutation.newUpdateBuilder(table);
            case INSERT_OR_UPDATE:
                return Mutation.newInsertOrUpdateBuilder(table);
            case REPLACE:
                return Mutation.newReplaceBuilder(table);
            case DELETE:
                throw new IllegalArgumentException("MutationOP(for insert) must not be DELETE!");
            default:
                return Mutation.newInsertOrUpdateBuilder(table);
        }
    }

}
