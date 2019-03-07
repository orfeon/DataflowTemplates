package net.orfeon.cloud.dataflow.util;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.tool.CreateRandomFileTool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class DummyGenericRecordGenerator {

    @Rule
    public final TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void generateDummyAvroFile() throws Exception {
        //
        final int count = 10;
        final String schemaFilePath = ClassLoader.getSystemResource("avro/dummy_schema.json").getPath();
        final String avroFilePath = "{output path}";

        List<GenericRecord> records = generate(schemaFilePath, count, tmpDir.newFile());
        final Schema schema = records.get(0).getSchema();
        try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, new File(avroFilePath));
            records.stream().forEach(record -> {
                try {
                    writer.append(record);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

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
                    record.put(field.name(), crop((Integer)record.get(field.name()), -719162, 0));
                } else if(LogicalTypes.timeMillis().equals(type.getLogicalType())) {
                    record.put(field.name(), crop((Integer)record.get(field.name()), 0, 86399999));
                }
                break;
            case LONG:
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    record.put(field.name(), crop((Long)record.get(field.name()), -719162L, 0L));
                } else if(LogicalTypes.timeMicros().equals(type.getLogicalType())) {
                    record.put(field.name(), crop((Long)record.get(field.name()), 0L, 86399999999L));
                }
                break;
            case FIXED:
                final int precisionFixed = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scaleFixed = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precisionFixed, scaleFixed).equals(type.getLogicalType())) {
                    GenericData.Fixed fixed = (GenericData.Fixed)record.get(field.name());
                    final byte[] bytes = fixed.bytes();
                    if(bytes.length == 0) {
                        BigDecimal bd = BigDecimal.valueOf(0, scaleFixed);
                        //record.put(field.name(), new GenericData.Fixed(fixed.getSchema(), bd.toBigInteger().toByteArray()));
                    } else if(new BigDecimal(new BigInteger(bytes), scaleFixed).precision() != precisionFixed) {
                        final BigDecimal bd2 = generateBigDecimal(precisionFixed, scaleFixed);
                        //record.put(field.name(), new GenericData.Fixed(fixed.getSchema(), bd2.toBigInteger().toByteArray()));
                    }
                }
                break;
            case BYTES:
                final int precisionBytes = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scaleBytes = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precisionBytes, scaleBytes).equals(type.getLogicalType())) {
                    final byte[] bytes = ((ByteBuffer)record.get(field.name())).array();
                    if(bytes.length == 0) {
                        record.put(field.name(), ByteBuffer.wrap(BigDecimal.valueOf(0, scaleBytes).toBigInteger().toByteArray()));
                    } else if(new BigDecimal(new BigInteger(bytes), scaleBytes).precision() != precisionBytes) {
                        final BigDecimal bd2 = generateBigDecimal(precisionBytes, scaleBytes);
                        record.put(field.name(), ByteBuffer.wrap(bd2.toBigInteger().toByteArray()));
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
                        is.add(crop((Integer)o, -719162, 0));
                    }
                    record.put(field.name(), is);
                } else if(LogicalTypes.timeMillis().equals(type.getLogicalType())) {
                    final List<Integer> is = new ArrayList<>();
                    for(final Object o : values) {
                        is.add(crop((Integer)o, 0, 86399999));
                    }
                    record.put(field.name(), is);
                }
                break;
            case LONG:
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    final List<Long> is = new ArrayList<>();
                    for(final Object o : values) {
                        is.add(crop((Long)o, -719162L, 0L));
                    }
                    record.put(field.name(), is);
                } else if(LogicalTypes.timeMicros().equals(type.getLogicalType())) {
                    final List<Long> is = new ArrayList<>();
                    for(final Object o : values) {
                        is.add(crop((Long)o, 0L, 86399999999L));
                    }
                    record.put(field.name(), is);
                }
                break;
            case FIXED:
                final int precisionFixed = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scaleFixed = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precisionFixed, scaleFixed).equals(type.getLogicalType())) {
                    final List<GenericData.Fixed> fixeds = new ArrayList<>();
                    for(final Object value : values) {
                        final GenericData.Fixed fixed = (GenericData.Fixed)value;
                        final byte[] bytes = fixed.bytes();
                        if(bytes.length == 0) {
                            BigDecimal bf2 = generateBigDecimal(precisionFixed, scaleFixed);
                            fixeds.add(new GenericData.Fixed(field.schema(), bf2.toBigInteger().toByteArray()));
                            fixeds.add(fixed);
                        } else if(new BigDecimal(new BigInteger(bytes), scaleFixed).precision() > precisionFixed) {
                            BigDecimal bf2 = generateBigDecimal(precisionFixed, scaleFixed);
                            fixeds.add(new GenericData.Fixed(field.schema(), bf2.toBigInteger().toByteArray()));
                            fixeds.add(fixed);
                        } else {
                            fixeds.add(fixed);
                        }
                    }
                    //record.put(field.name(), fixeds);
                }
                break;
            case BYTES:
                final int precisionBytes = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scaleBytes = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precisionBytes, scaleBytes).equals(type.getLogicalType())) {
                    final List<ByteBuffer> buffers = new ArrayList<>();
                    for(final Object o : values) {
                        final byte[] bytes = ((ByteBuffer)o).array();
                        if(bytes.length == 0) {
                            BigDecimal bf2 = generateBigDecimal(precisionBytes, scaleBytes);
                            buffers.add(ByteBuffer.wrap(bf2.toBigInteger().toByteArray()));
                        } else if(new BigDecimal(new BigInteger(bytes), scaleBytes).precision() > precisionBytes) {
                            BigDecimal bf2 = generateBigDecimal(precisionBytes, scaleBytes);
                            buffers.add(ByteBuffer.wrap(bf2.toBigInteger().toByteArray()));
                        } else {
                            buffers.add(ByteBuffer.wrap(bytes));
                        }
                    }
                    record.put(field.name(), buffers);
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

    private static BigDecimal generateBigDecimal(int precision, int scale) {
        StringBuilder sb = new StringBuilder();
        for(int i=precision; i>0; i--) {
            if(i == scale) {
                sb.append(".");
            }
            sb.append("1");
        }
        return new BigDecimal(sb.toString());
    }
}
