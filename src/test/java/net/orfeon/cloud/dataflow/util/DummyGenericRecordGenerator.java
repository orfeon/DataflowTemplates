package net.orfeon.cloud.dataflow.util;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.tool.CreateRandomFileTool;

import java.io.File;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class DummyGenericRecordGenerator {

    public static List<GenericRecord> generate(final String schemaFilePath, final int count, final File tmpOutput) throws Exception {
        final List<String> args = new ArrayList<>();
        args.add("--schema-file");
        args.add(schemaFilePath);
        args.add("--count");
        args.add(Integer.toString(count));
        args.add(tmpOutput.getPath());

        new CreateRandomFileTool().run(null, System.out, System.err, args);

        final DatumReader<GenericRecord> dataReader = new GenericDatumReader<>();
        try(final DataFileReader<GenericRecord> recordReader = new DataFileReader<>(tmpOutput, dataReader)) {
            final List<GenericRecord> records = new ArrayList<>();
            while (recordReader.hasNext()) {
                final GenericRecord record = recordReader.next();
                for(final Schema.Field field : record.getSchema().getFields()) {
                    modifyLogicalFieldValue(field, field.schema(), record);
                }
                records.add(record);
            }
            return records;
        }
    }


    private static void modifyLogicalFieldValue(final Schema.Field field, final Schema type, final GenericRecord record) {
        if(record.get(field.name()) == null) {
            return;
        }
        switch (type.getType()) {
            case INT:
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    record.put(field.name(), crop((Integer)record.get(field.name()), -719162, 100000));
                }
                break;
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    final ByteBuffer bf = (ByteBuffer)record.get(field.name());
                    if(bf.array().length == 0) {
                        record.put(field.name(), ByteBuffer.wrap(BigDecimal.valueOf(0, scale).toBigInteger().toByteArray()));
                    }
                }
                break;
            case UNION:
                for(final Schema childSchema : type.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    modifyLogicalFieldValue(field, childSchema, record);
                }
                break;
            case RECORD:
                final GenericRecord childRecord = (GenericRecord) record.get(field.name());
                for(final Schema.Field childField : childRecord.getSchema().getFields()) {
                    modifyLogicalFieldValue(childField, childField.schema(), childRecord);
                }
                break;
            case ARRAY:
                modifyLogicalArrayFieldValue(field, type.getElementType(), record);
                break;
            default:
                break;
        }
    }

    private static void modifyLogicalArrayFieldValue(final Schema.Field field, final Schema type, final GenericRecord record) {
        Iterable values = ((Iterable)record.get(field.name()));
        switch (type.getType()) {
            case INT:
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    final List<Integer> is = new ArrayList<>();
                    for(final Object o : values) {
                        is.add(crop((Integer)o, -719162, 100000));
                    }
                    record.put(field.name(), is);
                }
                break;
            case BYTES:
                final int precision_ = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale_ = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precision_, scale_).equals(type.getLogicalType())) {
                    final List<ByteBuffer> is = new ArrayList<>();
                    for(final Object o : values) {
                        final ByteBuffer bf = (ByteBuffer)o;
                        if(bf.array().length == 0) {
                            is.add(ByteBuffer.wrap(BigDecimal.valueOf(0, scale_).toBigInteger().toByteArray()));
                        } else {
                            is.add(bf);
                        }
                    }
                    record.put(field.name(), is);
                }
                break;
            case UNION:
                for(final Schema childSchema : type.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    modifyLogicalArrayFieldValue(field, childSchema, record);
                }
                break;
            case RECORD:
                for(final Object o : values) {
                    final GenericRecord r = (GenericRecord) o;
                    for(final Schema.Field childField : r.getSchema().getFields()) {
                        modifyLogicalFieldValue(childField, childField.schema(), r);
                    }
                }
                break;
            case ARRAY:
                break;
            default:
                break;
        }

    }

    private static <T extends Comparable> T crop(final T value, final T min, final T max) {
        if(value == null) {
            return value;
        }
        if(value.compareTo(min) > 0) {
            return min;
        }
        if(value.compareTo(max) < 0) {
            return max;
        }
        return value;
    }
}
