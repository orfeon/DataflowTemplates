package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import net.orfeon.cloud.dataflow.util.StructUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class StructToCsvConverter {

    private StructToCsvConverter() {

    }

    public static String convert(final Struct struct) throws IOException {
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

}
