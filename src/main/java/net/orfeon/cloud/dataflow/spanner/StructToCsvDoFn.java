package net.orfeon.cloud.dataflow.spanner;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class StructToCsvDoFn extends DoFn<Struct, String> {

    private Gson gson;

    @Setup
    public void setup() {
        this.gson = new Gson();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        Struct struct = c.element();
        List<Object> objs = struct.getType().getStructFields()
                .stream()
                .map((Type.StructField field) -> getFieldValue(field, struct, false))
                .collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        try(CSVPrinter printer = new CSVPrinter(sb, CSVFormat.DEFAULT)) {
            printer.printRecord(objs);
            printer.flush();
            c.output(sb.toString().trim());
        }
    }

    private Object getFieldValue(Type.StructField field, Struct struct, boolean isListField) {
        Type.Code code = isListField ? field.getType().getArrayElementType().getCode() : field.getType().getCode();
        switch (code) {
            case BOOL:
                return !isListField ? struct.getBoolean(field.getName()) : this.gson.toJson(struct.getBooleanList(field.getName()));
            case INT64:
                return !isListField ? struct.getLong(field.getName()) : this.gson.toJson(struct.getLongList(field.getName()));
            case FLOAT64:
                return !isListField ? struct.getDouble(field.getName()) : this.gson.toJson(struct.getDoubleList(field.getName()));
            case STRING:
                return !isListField ? struct.getString(field.getName()) : this.gson.toJson(struct.getStringList(field.getName()));
            case BYTES:
                return !isListField ? struct.getBytes(field.getName()) : this.gson.toJson(struct.getBytesList(field.getName()));
            case TIMESTAMP:
                return !isListField ? struct.getTimestamp(field.getName()) : this.gson.toJson(struct.getTimestampList(field.getName()));
            case DATE:
                return !isListField ? struct.getDate(field.getName()) : this.gson.toJson(struct.getDateList(field.getName()));
            case STRUCT:
                List<Struct> structs = struct.getStructList(field.getName());
                return !isListField && structs.size() > 0 ? structs.get(0) : this.gson.toJson(structs);
            case ARRAY:
                return getFieldValue(field, struct, true);
        }
        return null;
    }

}
