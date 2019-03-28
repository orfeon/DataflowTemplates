package net.orfeon.cloud.dataflow.transforms;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.util.AvroSchemaUtil;
import net.orfeon.cloud.dataflow.util.StructUtil;
import net.orfeon.cloud.dataflow.util.converter.StructToRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class StructToAvroTransform extends PTransform<PCollection<Struct>, WriteFilesResult<String>> {

    private static final String DEFAULT_KEY = "__KEY__";

    private static final Logger LOG = LoggerFactory.getLogger(StructToAvroTransform.class);

    public static final TupleTag<KV<String,Struct>> tagMain = new TupleTag<KV<String,Struct>>(){ private static final long serialVersionUID = 1L; };
    public static final TupleTag<KV<String,Struct>> tagStruct = new TupleTag<KV<String,Struct>>(){ private static final long serialVersionUID = 1L; };

    private final ValueProvider<String> output;
    private final ValueProvider<String> keyField;
    private final ValueProvider<Boolean> useSnappy;

    public StructToAvroTransform(ValueProvider<String> output, ValueProvider<String> keyField, ValueProvider<Boolean> useSnappy) {
        this.output = output;
        this.keyField = keyField;
        this.useSnappy = useSnappy;
    }

    public final WriteFilesResult<String> expand(PCollection<Struct> input) {

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

        return records.get(tagMain)
                .apply("WriteStructDynamically", FileIO.<String, KV<String, Struct>>writeDynamic()
                        .by(Contextful.fn((element) -> element.getKey()))
                        .via(Contextful.fn((key, c) -> {
                            final Map<String, Iterable<Struct>> sampleStruct = c.sideInput(schemaView);
                            if(!sampleStruct.containsKey(key) || !sampleStruct.get(key).iterator().hasNext()) {
                                throw new IllegalArgumentException(String.format("No matched struct to key %s !", key));
                            }
                            final Struct struct = sampleStruct.get(key).iterator().next();
                            final Schema schema = AvroSchemaUtil.convertSchema(struct);
                            final AvroIO.Sink<KV<String,Struct>> avroSink = AvroIO
                                    .sinkViaGenericRecords(schema,
                                            (KV<String, Struct> rstruct, Schema rschema) ->
                                                    StructToRecordConverter.convert(rstruct.getValue(), rschema));
                            if(useSnappy.get()) {
                                return avroSink.withCodec(CodecFactory.snappyCodec());
                            }
                            return avroSink;
                        }, Requirements.requiresSideInputs(schemaView)))
                        .withNaming(key -> FileIO.Write.defaultNaming(
                                buildPrefixFileName(this.output.get(), key), ".avro"))
                        .to(ValueProvider.NestedValueProvider.of(this.output, s -> buildPrefixDirName(s)))
                        .withDestinationCoder(StringUtf8Coder.of()));
    }

    private static String buildPrefixDirName(String output) {
        final boolean isgcs = output.startsWith("gs://");
        final String[] paths = output.replaceAll("gs://", "").split("/", -1);
        final StringBuilder sb = new StringBuilder(isgcs ? "gs://" : "");
        final int end = Math.max(paths.length-1, 1);
        for(int i=0; i<end; i++) {
            sb.append(paths[i]);
            sb.append("/");
        }
        return sb.toString();
    }

    private static String buildPrefixFileName(String output, String key) {
        final String prefix = DEFAULT_KEY.equals(key) ? "" : key;
        final String[] paths = output.replaceAll("gs://", "").split("/", -1);
        if(paths.length > 1) {
            return paths[paths.length-1] + prefix;
        }
        return prefix;
    }

}
