package net.orfeon.cloud.dataflow.spanner;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.sql.*;
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
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final String name = field.getName();
            if("f".equals(name)) {
                throw new IllegalArgumentException("Struct must not have field name f because `f` is reserved tablerow field name.");
            }
            Object value = getFieldValue(field, struct);
            if(value == null) {
                continue;
            }
            if(Type.timestamp().equals(field.getType())) {
                value = ((com.google.cloud.Timestamp)value).getSeconds();
            } else if(Type.date().equals(field.getType())) {
                value = value.toString();
            }
            row.set(name, value);
        }
        return row;
    }

    public static Mutation toMutation(final Struct struct, final String table) {
        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
        for(Type.StructField field : struct.getType().getStructFields()) {
            switch(field.getType().getCode()) {
                case STRING:
                    builder = builder.set(field.getName()).to(struct.isNull(field.getName()) ? null : struct.getString(field.getName()));
                    break;
                case BYTES:
                    builder = builder.set(field.getName()).to(struct.isNull(field.getName()) ? null : struct.getBytes(field.getName()));
                    break;
                case BOOL:
                    builder = builder.set(field.getName()).to(struct.isNull(field.getName()) ? null : struct.getBoolean(field.getName()));
                    break;
                case INT64:
                    builder = builder.set(field.getName()).to(struct.isNull(field.getName()) ? null : struct.getLong(field.getName()));
                    break;
                case FLOAT64:
                    builder = builder.set(field.getName()).to(struct.isNull(field.getName()) ? null : struct.getDouble(field.getName()));
                    break;
                case DATE:
                    builder = builder.set(field.getName()).to(struct.isNull(field.getName()) ? null : struct.getDate(field.getName()));
                    break;
                case TIMESTAMP:
                    builder = builder.set(field.getName()).to(struct.isNull(field.getName()) ? null : struct.getTimestamp(field.getName()));
                    break;
                case STRUCT:
                    builder = builder.set(field.getName()).to(struct.isNull(field.getName()) ? null : struct.getStruct(field.getName()));
                    break;
                case ARRAY:
                    switch (field.getType().getArrayElementType().getCode()) {
                        case STRING:
                            builder = builder.set(field.getName()).toStringArray(struct.isNull(field.getName()) ? null : struct.getStringList(field.getName()));
                            break;
                        case BYTES:
                            builder = builder.set(field.getName()).toBytesArray(struct.isNull(field.getName()) ? null : struct.getBytesList(field.getName()));
                            break;
                        case BOOL:
                            builder = builder.set(field.getName()).toBoolArray(struct.isNull(field.getName()) ? null : struct.getBooleanArray(field.getName()));
                            break;
                        case INT64:
                            builder = builder.set(field.getName()).toInt64Array(struct.isNull(field.getName()) ? null : struct.getLongArray(field.getName()));
                            break;
                        case FLOAT64:
                            builder = builder.set(field.getName()).toFloat64Array(struct.isNull(field.getName()) ? null : struct.getDoubleArray(field.getName()));
                            break;
                        case DATE:
                            builder = builder.set(field.getName()).toDateArray(struct.isNull(field.getName()) ? null : struct.getDateList(field.getName()));
                            break;
                        case TIMESTAMP:
                            builder = builder.set(field.getName()).toTimestampArray(struct.isNull(field.getName()) ? null : struct.getTimestampList(field.getName()));
                            break;
                        case STRUCT:
                            // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2018/10/26)
                            //builder = builder.set(field.getName()).toStructArray(struct.getStructList(field.getName()));
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
            switch (meta.getColumnType(column)) {
                case Types.CHAR:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getString(column));
                    break;
                case Types.NUMERIC:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getBigDecimal(column).doubleValue());
                    break;
                case Types.DECIMAL:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getBigDecimal(column).doubleValue());
                    break;
                case Types.INTEGER:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getInt(column));
                    break;
                case Types.SMALLINT:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getShort(column));
                    break;
                case Types.FLOAT:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getFloat(column));
                    break;
                case Types.REAL:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getFloat(column));
                    break;
                case Types.DOUBLE:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getDouble(column));
                    break;
                case Types.VARCHAR:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getString(column));
                    break;
                case Types.BOOLEAN:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getBoolean(column));
                    break;
                case Types.DATALINK:
                    break;
                case Types.DATE:
                    final LocalDate localDate = resultSet.getDate(column).toLocalDate();
                    builder = builder.set(meta.getColumnName(column)).to(Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));
                    break;
                case Types.TIME:
                    final Time time = resultSet.getTime(column);
                    builder = builder.set(meta.getColumnName(column)).to(time.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
                    break;
                case Types.TIMESTAMP:
                    builder = builder.set(meta.getColumnName(column)).to(Timestamp.of(resultSet.getTimestamp(column)));
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
                    builder = builder.set(meta.getColumnName(column)).to(timeWT.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    builder = builder.set(meta.getColumnName(column)).to(Timestamp.of(resultSet.getTimestamp(column)));
                    break;
                case Types.LONGVARCHAR:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getString(column));
                    break;
                case Types.BINARY:
                    builder = builder.set(meta.getColumnName(column)).to(ByteArray.copyFrom(resultSet.getBytes(column)));
                    break;
                case Types.VARBINARY:
                    builder = builder.set(meta.getColumnName(column)).to(ByteArray.copyFrom(resultSet.getBytes(column)));
                    break;
                case Types.LONGVARBINARY:
                    builder = builder.set(meta.getColumnName(column)).to(ByteArray.copyFrom(resultSet.getBytes(column)));
                    break;
                case Types.BIGINT:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getLong(column));
                    break;
                case Types.TINYINT:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getByte(column));
                    break;
                case Types.BIT:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getBoolean(column));
                    break;
                case Types.ROWID:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getRowId(column).toString());
                    break;
                case Types.NVARCHAR:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getNString(column));
                    break;
                case Types.NCHAR:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getNString(column));
                    break;
                case Types.LONGNVARCHAR:
                    builder = builder.set(meta.getColumnName(column)).to(resultSet.getNString(column));
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
        switch (field.getType().getCode()) {
            case BOOL:
                obj.addProperty(field.getName(), struct.isNull(field.getName()) ? null : struct.getBoolean(field.getName()));
                break;
            case INT64:
                obj.addProperty(field.getName(), struct.isNull(field.getName()) ? null : struct.getLong(field.getName()));
                break;
            case FLOAT64:
                obj.addProperty(field.getName(), struct.isNull(field.getName()) ? null : struct.getDouble(field.getName()));
                break;
            case STRING:
                obj.addProperty(field.getName(), struct.isNull(field.getName()) ? null : struct.getString(field.getName()));
                break;
            case BYTES:
                obj.addProperty(field.getName(), struct.isNull(field.getName()) ? null : struct.getBytes(field.getName()).toBase64());
                break;
            case TIMESTAMP:
                obj.addProperty(field.getName(), struct.isNull(field.getName()) ? null : struct.getTimestamp(field.getName()).toString());
                break;
            case DATE:
                obj.addProperty(field.getName(), struct.isNull(field.getName()) ? null : struct.getDate(field.getName()).toString());
                break;
            case STRUCT:
                if(struct.isNull(field.getName())) {
                    obj.add(field.getName(), null);
                    return;
                }
                Struct childStruct = struct.getStruct(field.getName());
                JsonObject childObj = new JsonObject();
                for(Type.StructField childField : childStruct.getType().getStructFields()) {
                    setJsonFieldValue(childObj, childField, childStruct);
                }
                obj.add(field.getName(), childObj);
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

}
