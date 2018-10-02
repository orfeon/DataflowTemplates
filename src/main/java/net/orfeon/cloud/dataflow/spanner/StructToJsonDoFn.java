package net.orfeon.cloud.dataflow.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.transforms.DoFn;

public class StructToJsonDoFn extends DoFn<Struct, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        Struct struct = c.element();
        JsonObject obj = new JsonObject();
        for(Type.StructField field : struct.getType().getStructFields()) {
            setFieldValue(obj, field, struct, false);
        }
        c.output(obj.toString());
    }

    private void setFieldValue(JsonObject obj, Type.StructField field, Struct struct, boolean isListField) {
        Type.Code code = isListField ? field.getType().getArrayElementType().getCode() : field.getType().getCode();
        switch (code) {
            case BOOL:
                if(isListField) {
                    JsonArray array = new JsonArray();
                    struct.getBooleanList(field.getName()).stream().forEach(array::add);
                    obj.add(field.getName(), array);
                } else {
                    obj.addProperty(field.getName(), struct.getBoolean(field.getName()));
                }
                break;
            case INT64:
                if(isListField) {
                    JsonArray array = new JsonArray();
                    struct.getLongList(field.getName()).stream().forEach(array::add);
                    obj.add(field.getName(), array);
                } else {
                    obj.addProperty(field.getName(), struct.getLong(field.getName()));
                }
                break;
            case FLOAT64:
                if(isListField) {
                    JsonArray array = new JsonArray();
                    struct.getDoubleList(field.getName()).stream().forEach(array::add);
                    obj.add(field.getName(), array);
                } else {
                    obj.addProperty(field.getName(), struct.getDouble(field.getName()));
                }
                break;
            case STRING:
                if(isListField) {
                    JsonArray array = new JsonArray();
                    struct.getStringList(field.getName()).stream().forEach(array::add);
                    obj.add(field.getName(), array);
                } else {
                    obj.addProperty(field.getName(), struct.getString(field.getName()));
                }
                break;
            case BYTES:
                if(isListField) {
                    JsonArray array = new JsonArray();
                    struct.getBytesList(field.getName()).stream().map((ByteArray::toBase64)).forEach(array::add);
                    obj.add(field.getName(), array);
                } else {
                    obj.addProperty(field.getName(), struct.getBytes(field.getName()).toBase64());
                }
                break;
            case TIMESTAMP:
                if(isListField) {
                    JsonArray array = new JsonArray();
                    struct.getTimestampList(field.getName()).stream().map((com.google.cloud.Timestamp::getNanos)).forEach(array::add);
                    obj.add(field.getName(), array);
                } else {
                    obj.addProperty(field.getName(), struct.getTimestamp(field.getName()).getNanos());
                }
                break;
            case DATE:
                if(isListField) {
                    JsonArray array = new JsonArray();
                    struct.getDateList(field.getName()).stream().map((Date date) -> date.toString()).forEach(array::add);
                    obj.add(field.getName(), array);
                } else {
                    obj.addProperty(field.getName(), struct.getBytes(field.getName()).toBase64());
                }
                break;
            case STRUCT:
                JsonArray array = new JsonArray();
                for(Struct childStruct : struct.getStructList(field.getName())) {
                    JsonObject childObj = new JsonObject();
                    for(Type.StructField childField : childStruct.getType().getStructFields()) {
                        setFieldValue(childObj, childField, childStruct, false);
                    }
                    array.add(childObj);
                }
                obj.add(field.getName(), array);
                break;
            case ARRAY:
                setFieldValue(obj, field, struct, true);
                return;
        }
    }

}
