package net.orfeon.cloud.dataflow.spanner;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.Struct;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.sql.*;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class StructUtil {

    private StructUtil() {

    }

    public static String toJson(final Struct struct) {
        JsonObject obj = new JsonObject();
        struct.getType().getStructFields().stream()
                .forEach(f -> setJsonFieldValue(obj, f, struct));
        return obj.toString();
    }

    public static String toCsvLine(final Struct struct) throws IOException {
        List<Object> objs = struct.getType().getStructFields()
                .stream()
                .map((Type.StructField field) -> StructUtil.getFieldValue(field, struct))
                .collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        try(CSVPrinter printer = new CSVPrinter(sb, CSVFormat.DEFAULT)) {
            printer.printRecord(objs);
            printer.flush();
            return sb.toString().trim();
        }
    }

    public static TableRow toTableRow(final Struct struct) {
        final TableRow row = new TableRow();
        for (final Type.StructField field : struct.getType().getStructFields()) {
            final String name = field.getName();
            if ("f".equals(name)) {
                throw new IllegalArgumentException("Struct must not have field name f because `f` is reserved tablerow field name.");
            }
            Object value = getFieldValue(field, struct);
            if (value == null) {
                continue;
            }
            if (Type.timestamp().equals(field.getType())) {
                value = ((com.google.cloud.Timestamp) value).getSeconds();
            } else if (Type.date().equals(field.getType())) {
                value = value.toString();
            }
            row.set(name, value);
        }
        return row;
    }

    public static Mutation toMutation(final Struct struct, final String table, final Mutation.Op mutationOp) {
        Mutation.WriteBuilder builder = createMutationWriteBuilder(table, mutationOp);
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final String fieldName = field.getName();
            final boolean isNullField = struct.isNull(fieldName);
            final ValueBinder<Mutation.WriteBuilder> binder = builder.set(field.getName());
            switch(field.getType().getCode()) {
                case STRING:
                    builder = binder.to(isNullField ? null : struct.getString(fieldName));
                    break;
                case BYTES:
                    builder = binder.to(isNullField ? null : struct.getBytes(fieldName));
                    break;
                case BOOL:
                    builder = binder.to(isNullField ? null : struct.getBoolean(fieldName));
                    break;
                case INT64:
                    builder = binder.to(isNullField ? null : struct.getLong(fieldName));
                    break;
                case FLOAT64:
                    builder = binder.to(isNullField ? null : struct.getDouble(fieldName));
                    break;
                case DATE:
                    builder = binder.to(isNullField ? null : struct.getDate(fieldName));
                    break;
                case TIMESTAMP:
                    builder = binder.to(isNullField ? null : struct.getTimestamp(fieldName));
                    break;
                case STRUCT:
                    builder = binder.to(isNullField ? null : struct.getStruct(fieldName));
                    break;
                case ARRAY:
                    switch (field.getType().getArrayElementType().getCode()) {
                        case STRING:
                            builder = binder.toStringArray(isNullField ? null : struct.getStringList(fieldName));
                            break;
                        case BYTES:
                            builder = binder.toBytesArray(isNullField ? null : struct.getBytesList(fieldName));
                            break;
                        case BOOL:
                            builder = binder.toBoolArray(isNullField ? null : struct.getBooleanArray(fieldName));
                            break;
                        case INT64:
                            builder = binder.toInt64Array(isNullField ? null : struct.getLongArray(fieldName));
                            break;
                        case FLOAT64:
                            builder = binder.toFloat64Array(isNullField ? null : struct.getDoubleArray(fieldName));
                            break;
                        case DATE:
                            builder = binder.toDateArray(isNullField ? null : struct.getDateList(fieldName));
                            break;
                        case TIMESTAMP:
                            builder = binder.toTimestampArray(isNullField ? null : struct.getTimestampList(fieldName));
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

    public static Struct from(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int columnCount = meta.getColumnCount();
        Struct.Builder builder = Struct.newBuilder();
        for (int column = 1; column <= columnCount; ++column) {
            final ValueBinder<Struct.Builder> binder = builder.set(meta.getColumnName(column));
            switch (meta.getColumnType(column)) {
                case Types.CHAR:
                    builder = binder.to(resultSet.getString(column));
                    break;
                case Types.NUMERIC:
                    builder = binder.to(resultSet.getBigDecimal(column).doubleValue());
                    break;
                case Types.DECIMAL:
                    builder = binder.to(resultSet.getBigDecimal(column).doubleValue());
                    break;
                case Types.INTEGER:
                    builder = binder.to(resultSet.getInt(column));
                    break;
                case Types.SMALLINT:
                    builder = binder.to(resultSet.getShort(column));
                    break;
                case Types.FLOAT:
                    builder = binder.to(resultSet.getFloat(column));
                    break;
                case Types.REAL:
                    builder = binder.to(resultSet.getFloat(column));
                    break;
                case Types.DOUBLE:
                    builder = binder.to(resultSet.getDouble(column));
                    break;
                case Types.VARCHAR:
                    builder = binder.to(resultSet.getString(column));
                    break;
                case Types.BOOLEAN:
                    builder = binder.to(resultSet.getBoolean(column));
                    break;
                case Types.DATALINK:
                    break;
                case Types.DATE:
                    final LocalDate localDate = resultSet.getDate(column).toLocalDate();
                    final Date date = Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
                    builder = binder.to(date);
                    break;
                case Types.TIME:
                    final Time time = resultSet.getTime(column);
                    builder = binder.to(time.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
                    break;
                case Types.TIMESTAMP:
                    builder = binder.to(Timestamp.of(resultSet.getTimestamp(column)));
                    break;
                case Types.JAVA_OBJECT:
                    break;
                case Types.DISTINCT:
                    break;
                case Types.STRUCT:
                    break;
                case Types.ARRAY:
                    break;
                case Types.BLOB:
                    break;
                case Types.CLOB:
                    break;
                case Types.REF:
                    break;
                case Types.SQLXML:
                    break;
                case Types.NCLOB:
                    break;
                case Types.REF_CURSOR:
                    break;
                case Types.TIME_WITH_TIMEZONE:
                    final Time timeWT = resultSet.getTime(column);
                    builder = binder.to(timeWT.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    builder = binder.to(Timestamp.of(resultSet.getTimestamp(column)));
                    break;
                case Types.LONGVARCHAR:
                    builder = binder.to(resultSet.getString(column));
                    break;
                case Types.BINARY:
                    builder = binder.to(ByteArray.copyFrom(resultSet.getBytes(column)));
                    break;
                case Types.VARBINARY:
                    builder = binder.to(ByteArray.copyFrom(resultSet.getBytes(column)));
                    break;
                case Types.LONGVARBINARY:
                    builder = binder.to(ByteArray.copyFrom(resultSet.getBytes(column)));
                    break;
                case Types.BIGINT:
                    builder = binder.to(resultSet.getLong(column));
                    break;
                case Types.TINYINT:
                    builder = binder.to(resultSet.getByte(column));
                    break;
                case Types.BIT:
                    builder = binder.to(resultSet.getBoolean(column));
                    break;
                case Types.ROWID:
                    builder = binder.to(resultSet.getRowId(column).toString());
                    break;
                case Types.NVARCHAR:
                    builder = binder.to(resultSet.getNString(column));
                    break;
                case Types.NCHAR:
                    builder = binder.to(resultSet.getNString(column));
                    break;
                case Types.LONGNVARCHAR:
                    builder = binder.to(resultSet.getNString(column));
                    break;
            }
        }
        return builder.build();
    }

    public static List<Struct> from(final MutationGroup mutationGroup) {
        final List<Struct> structs = mutationGroup.attached().stream()
                .map(StructUtil::from)
                .collect(Collectors.toList());
        structs.add(StructUtil.from(mutationGroup.primary()));
        return structs;
    }

    public static Struct from(final Mutation mutation) {
        Struct.Builder builder = Struct.newBuilder();
        for(final Map.Entry<String, Value> column : mutation.asMap().entrySet()) {
            final Value value = column.getValue();
            final ValueBinder<Struct.Builder> binder = builder.set(column.getKey());
            switch(value.getType().getCode()) {
                case DATE:
                    builder = binder.to(value.isNull() ? null : value.getDate());
                    break;
                case INT64:
                    builder = binder.to(value.isNull() ? null : value.getInt64());
                    break;
                case STRING:
                    builder = binder.to(value.isNull() ? null : value.getString());
                    break;
                case TIMESTAMP:
                    builder = binder.to(value.isNull() ? null : value.getTimestamp());
                    break;
                case BOOL:
                    builder = binder.to(value.isNull() ? null : value.getBool());
                    break;
                case BYTES:
                    builder = binder.to(value.isNull() ? null : value.getBytes());
                    break;
                case FLOAT64:
                    builder = binder.to(value.isNull() ? null : value.getFloat64());
                    break;
                case STRUCT:
                    builder = binder.to(value.isNull() ? null : value.getStruct());
                    break;
                case ARRAY:
                    switch (value.getType().getArrayElementType().getCode()) {
                        case DATE:
                            builder = binder.toDateArray(value.isNull() ? null : value.getDateArray());
                            break;
                        case INT64:
                            builder = binder.toInt64Array(value.isNull() ? null : value.getInt64Array());
                            break;
                        case STRING:
                            builder = binder.toStringArray(value.isNull() ? null : value.getStringArray());
                            break;
                        case TIMESTAMP:
                            builder = binder.toTimestampArray(value.isNull() ? null : value.getTimestampArray());
                            break;
                        case BOOL:
                            builder = binder.toBoolArray(value.isNull() ? null : value.getBoolArray());
                            break;
                        case BYTES:
                            builder = binder.toBytesArray(value.isNull() ? null : value.getBytesArray());
                            break;
                        case FLOAT64:
                            builder = binder.toFloat64Array(value.isNull() ? null : value.getFloat64Array());
                            break;
                    }
                    break;
            }
        }
        return builder.build();
    }

    public static Object getFieldValue(final String fieldName, final Struct struct) {
        return struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny()
                .map(f -> getFieldValue(f, struct))
                .orElse(null);
    }

    private static Object getFieldValue(Type.StructField field, Struct struct) {
        if(struct.isNull(field.getName())) {
            return null;
        }
        switch (field.getType().getCode()) {
            case BOOL:
                return struct.getBoolean(field.getName());
            case INT64:
                return struct.getLong(field.getName());
            case FLOAT64:
                return struct.getDouble(field.getName());
            case STRING:
                return struct.getString(field.getName());
            case BYTES:
                return struct.getBytes(field.getName()).toBase64();
            case TIMESTAMP:
                return struct.getTimestamp(field.getName());
            case DATE:
                return struct.getDate(field.getName());
            case STRUCT:
                Map<String,Object> map = new HashMap<>();
                Struct childStruct = struct.getStruct(field.getName());
                for (Type.StructField childField : childStruct.getType().getStructFields()) {
                    map.put(field.getName(), getFieldValue(childField, childStruct));
                }
                return map;
            case ARRAY:
                return getArrayFieldValue(field, struct);
        }
        return null;
    }

    private static Object getArrayFieldValue(Type.StructField field, Struct struct) {
        List list = new ArrayList<>();
        switch (field.getType().getArrayElementType().getCode()) {
            case BOOL:
                struct.getBooleanList(field.getName()).stream().forEach(list::add);
                return list;
            case INT64:
                struct.getLongList(field.getName()).stream().forEach(list::add);
                return list;
            case FLOAT64:
                struct.getDoubleList(field.getName()).stream().forEach(list::add);
                return list;
            case STRING:
                struct.getStringList(field.getName()).stream().forEach(list::add);
                return list;
            case BYTES:
                struct.getBytesList(field.getName()).stream().map((ByteArray::toBase64)).forEach(list::add);
                return list;
            case TIMESTAMP:
                struct.getTimestampList(field.getName()).stream().forEach(list::add);
                return list;
            case DATE:
                struct.getDateList(field.getName()).stream().forEach(list::add);
                return list;
            case STRUCT:
                List<Map<String,Object>> maps = new ArrayList<>();
                for (Struct childStruct : struct.getStructList(field.getName())) {
                    Map<String,Object> map = new HashMap<>();
                    for (Type.StructField childField : childStruct.getType().getStructFields()) {
                        map.put(field.getName(), getFieldValue(childField, childStruct));
                    }
                    maps.add(map);
                }
                return maps;
            case ARRAY:
                return getArrayFieldValue(field, struct);
        }
        return null;
    }

    private static void setJsonFieldValue(JsonObject obj, Type.StructField field, Struct struct) {
        final String fieldName = field.getName();
        final boolean isNullField = struct.isNull(fieldName);
        switch (field.getType().getCode()) {
            case BOOL:
                obj.addProperty(fieldName, isNullField ? null : struct.getBoolean(fieldName));
                break;
            case INT64:
                obj.addProperty(fieldName, isNullField ? null : struct.getLong(fieldName));
                break;
            case FLOAT64:
                obj.addProperty(fieldName, isNullField ? null : struct.getDouble(fieldName));
                break;
            case STRING:
                obj.addProperty(fieldName, isNullField ? null : struct.getString(fieldName));
                break;
            case BYTES:
                obj.addProperty(fieldName, isNullField ? null : struct.getBytes(fieldName).toBase64());
                break;
            case TIMESTAMP:
                obj.addProperty(fieldName, isNullField ? null : struct.getTimestamp(fieldName).toString());
                break;
            case DATE:
                obj.addProperty(fieldName, isNullField ? null : struct.getDate(fieldName).toString());
                break;
            case STRUCT:
                if(isNullField) {
                    obj.add(field.getName(), null);
                    return;
                }
                Struct childStruct = struct.getStruct(fieldName);
                JsonObject childObj = new JsonObject();
                for(Type.StructField childField : childStruct.getType().getStructFields()) {
                    setJsonFieldValue(childObj, childField, childStruct);
                }
                obj.add(fieldName, childObj);
                break;
            case ARRAY:
                setJsonArrayFieldValue(obj, field, struct);
                break;
        }
    }

    private static void setJsonArrayFieldValue(JsonObject obj, Type.StructField field, Struct struct) {
        if(struct.isNull(field.getName())) {
            obj.add(field.getName(), null);
            return;
        }
        JsonArray array = new JsonArray();
        switch (field.getType().getArrayElementType().getCode()) {
            case BOOL:
                struct.getBooleanList(field.getName()).stream().forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case INT64:
                struct.getLongList(field.getName()).stream().forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case FLOAT64:
                struct.getDoubleList(field.getName()).stream().forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case STRING:
                struct.getStringList(field.getName()).stream().forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case BYTES:
                struct.getBytesList(field.getName()).stream().map(ByteArray::toBase64).forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case TIMESTAMP:
                struct.getTimestampList(field.getName()).stream().map(s -> s.toString()).forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case DATE:
                struct.getDateList(field.getName()).stream().map((Date date) -> date.toString()).forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case STRUCT:
                for(Struct childStruct : struct.getStructList(field.getName())) {
                    JsonObject childObj = new JsonObject();
                    for(Type.StructField childField : childStruct.getType().getStructFields()) {
                        setJsonFieldValue(childObj, childField, childStruct);
                    }
                    array.add(childObj);
                }
                obj.add(field.getName(), array);
                break;
            case ARRAY:
                setJsonArrayFieldValue(obj, field, struct);
                break;
        }
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
            default:
                return Mutation.newInsertOrUpdateBuilder(table);
        }
    }

}
