package net.orfeon.cloud.dataflow.spanner;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.storage.AvroUtil;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StructToAvroTransform extends PTransform<PCollection<Struct>, PDone> {

    private static final String DEFAULT_KEY = "__KEY__";

    public static final TupleTag<KV<String,Struct>> tagMain = new TupleTag<KV<String,Struct>>(){ private static final long serialVersionUID = 1L; };
    public static final TupleTag<KV<String,Struct>> tagStruct = new TupleTag<KV<String,Struct>>(){ private static final long serialVersionUID = 1L; };

    private final ValueProvider<String> output;
    private final ValueProvider<String> keyField;

    public StructToAvroTransform(ValueProvider<String> output, ValueProvider<String> keyField) {
        this.output = output;
        this.keyField = keyField;
    }

    public final PDone expand(PCollection<Struct> input) {

        PCollectionTuple records = input.apply("AddGroupingKey", ParDo.of(new DoFn<Struct, KV<String, Struct>>() {

            private String keyFieldString;
            private Set<String> check;

            @Setup
            public void setup() {
                this.keyFieldString = keyField.get();
                this.check = new HashSet<>();
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                Struct struct = c.element();
                String key = this.keyFieldString == null ? DEFAULT_KEY : StructUtil.getFieldValue(this.keyFieldString, struct).toString();
                KV<String, Struct> kv = KV.of(key, struct);
                c.output(kv);
                if(!this.check.contains(key)) {
                    c.output(tagStruct, kv);
                    this.check.add(key);
                }

            }

        }).withOutputTags(tagMain, TupleTagList.of(tagStruct)));

        PCollectionView<Map<String, Iterable<Struct>>> schemaView = records.get(tagStruct)
                .apply("SampleStructPerKey", Sample.fixedSizePerKey(1))
                .apply("ViewAsMap", View.asMap());

        WriteFilesResult<String> writeFilesResult = records.get(tagMain)
                .apply("WriteStructDynamically", FileIO.<String, KV<String, Struct>>writeDynamic()
                        .by(Contextful.fn((element) -> element.getKey()))
                        .via(Contextful.fn((key, c) -> {
                            final Map<String, Iterable<Struct>> sampleStruct = c.sideInput(schemaView);
                            if(!sampleStruct.containsKey(key) || !sampleStruct.get(key).iterator().hasNext()) {
                                throw new IllegalArgumentException(String.format("No matched struct to key %s !", key));
                            }
                            final Struct struct = sampleStruct.get(key).iterator().next();
                            final Schema schema = AvroUtil.convertSchemaFromStruct(struct);
                            return AvroIO.sinkViaGenericRecords(schema, (rstruct, rschema) -> AvroUtil.convertGenericRecord(rstruct.getValue(), rschema));
                        }, Requirements.requiresSideInputs(schemaView)))
                        .withNaming(key -> FileIO.Write.defaultNaming(DEFAULT_KEY.equals(key) ? "" : key, ".avro"))
                        .to(this.output)
                        .withDestinationCoder(StringUtf8Coder.of()));

        return PDone.in(writeFilesResult.getPipeline());
    }

}
