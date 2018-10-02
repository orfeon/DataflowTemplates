package net.orfeon.cloud.dataflow.spanner;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.*;

public class StructToTableRowDoFn extends DoFn<Struct, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        Struct struct = c.element();
        TableRow row = new TableRow();
        for(Type.StructField field : struct.getType().getStructFields()) {
            String name = field.getName();
            if("f".equals(name)) {
                continue;
            }
            Object value = getFieldValue(field, struct, false);
            row.set(name, value);
        }
        c.output(row);
    }

    private Object getFieldValue(Type.StructField field, Struct struct, boolean isListField) {
        Type.Code code = isListField ? field.getType().getArrayElementType().getCode() : field.getType().getCode();
        switch (code) {
            case BOOL:
                if (isListField) {
                    List<Boolean> list = new ArrayList<>();
                    struct.getBooleanList(field.getName()).stream().forEach(list::add);
                    return list;
                } else {
                    return struct.getBoolean(field.getName());
                }
            case INT64:
                if (isListField) {
                    List<Long> list = new ArrayList<>();
                    struct.getLongList(field.getName()).stream().forEach(list::add);
                    return list;
                } else {
                    return struct.getLong(field.getName());
                }
            case FLOAT64:
                if (isListField) {
                    List<Double> list = new ArrayList<>();
                    struct.getDoubleList(field.getName()).stream().forEach(list::add);
                    return list;
                } else {
                    return struct.getDouble(field.getName());
                }
            case STRING:
                if (isListField) {
                    List<String> list = new ArrayList<>();
                    struct.getStringList(field.getName()).stream().forEach(list::add);
                    return list;
                } else {
                    return struct.getString(field.getName());
                }
            case BYTES:
                if (isListField) {
                    List<String> list = new ArrayList<>();
                    struct.getBytesList(field.getName()).stream().map((ByteArray::toBase64)).forEach(list::add);
                    return list;
                } else {
                    return struct.getBytes(field.getName()).toBase64();
                }
            case TIMESTAMP:
                if (isListField) {
                    List<Long> list = new ArrayList<>();
                    struct.getTimestampList(field.getName()).stream().map((com.google.cloud.Timestamp::getSeconds)).forEach(list::add);
                    return list;
                } else {
                    return struct.getTimestamp(field.getName()).getSeconds();
                }
            case DATE:
                if (isListField) {
                    List<String> list = new ArrayList<>();
                    struct.getDateList(field.getName()).stream().map((Date date) -> date.toString()).forEach(list::add);
                    return list;
                } else {
                    return struct.getDate(field.getName());
                }
            case STRUCT:
                List<TableRow> childRows = new ArrayList<>();
                for (Struct childStruct : struct.getStructList(field.getName())) {
                    TableRow childRow = new TableRow();
                    for (Type.StructField childField : childStruct.getType().getStructFields()) {
                        childRow.set(field.getName(), getFieldValue(childField, childStruct, false));
                    }
                    childRows.add(childRow);
                }
                return childRows;
            case ARRAY:
                return getFieldValue(field, struct, true);
        }
        return null;
    }

    private void removeNullValue(TableRow row) {
        Set<String> removableKeys = new HashSet<>();
        for(Map.Entry<String, Object> entry : row.entrySet()) {
            if(entry.getValue() == null) {
                removableKeys.add(entry.getKey());
            }
        }
        for(String key : removableKeys) {
            row.remove(key);
        }
    }
}
