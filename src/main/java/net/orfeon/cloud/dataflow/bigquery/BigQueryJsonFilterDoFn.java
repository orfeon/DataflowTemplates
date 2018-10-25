package net.orfeon.cloud.dataflow.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.util.Map;

public class BigQueryRemoveFieldDoFn extends DoFn<TableRow, TableRow> {

    private Gson gson;

    @Setup
    public void setup() {
        this.gson = new Gson();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        final TableRow row = c.element();
        final String jsonString = (String)row.get("params");
        final JsonObject sourceObj = this.gson.fromJson(jsonString, JsonObject.class);
        final JsonObject targetObj = new JsonObject();
        removeField(sourceObj, targetObj);
        row.set("params", targetObj.toString());
        c.output(row);
    }

    private void removeField(JsonObject sourceObj, JsonObject targetObj) {
        for(final Map.Entry<String, JsonElement> entry : sourceObj.entrySet()) {
            if(entry.getValue().isJsonObject()) {
                final JsonObject childObj = new JsonObject();
                removeField(entry.getValue().getAsJsonObject(), childObj);
                targetObj.add(entry.getKey(), childObj);
                continue;
            }
            if(entry.getKey().equals("password")) {
                targetObj.addProperty(entry.getKey(), "[FILTERED]");
            } else {
                targetObj.add(entry.getKey(), entry.getValue());
            }
        }
    }
}
