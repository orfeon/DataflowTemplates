package net.orfeon.cloud.dataflow.util.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import net.orfeon.cloud.dataflow.util.StructUtil;

public class StructToTableRowConverter {

    private StructToTableRowConverter() {

    }

    public static TableRow convert(final Struct struct) {
        final TableRow row = new TableRow();
        for (final Type.StructField field : struct.getType().getStructFields()) {
            final String name = field.getName();
            if ("f".equals(name)) {
                throw new IllegalArgumentException("Struct must not have field name f because `f` is reserved tablerow field name.");
            }
            Object value = StructUtil.getFieldValue(field, struct);
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
}
