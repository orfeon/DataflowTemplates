package net.orfeon.cloud.dataflow.templates;

import net.orfeon.cloud.dataflow.transforms.BigQueryDirectIO;
import net.orfeon.cloud.dataflow.util.converter.RecordToTFRecordConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class BigQueryToTFRecord {

    public interface BigQueryToTFRecordPipelineOption extends PipelineOptions {

        @Description("SQL Query text to read records from BigQuery")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("GCS path to store tfrecord")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> output);

        @Description("Field in query results to separate records.")
        ValueProvider<String> getSeparateField();
        void setSeparateField(ValueProvider<String> output);

        @Description("Parallel num.")
        @Default.Integer(0)
        ValueProvider<Integer> getParallelNum();
        void setParallelNum(ValueProvider<Integer> parallelNum);
    }

    public static void main(final String[] args) {

        final BigQueryToTFRecordPipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(BigQueryToTFRecordPipelineOption.class);

        final Pipeline pipeline = Pipeline.create(options);

        final ValueProvider<String> keyFieldVP = options.getSeparateField();
        final ValueProvider<String> outputVP = options.getOutput();

        final TupleTag<KV<String,byte[]>> tagOutput = new TupleTag<KV<String,byte[]>>(){};
        pipeline.apply("QueryBigQuery", BigQueryDirectIO.read((SchemaAndRecord sr) -> {
                            final String keyField = keyFieldVP.get();
                            if(keyField == null) {
                                return KV.of("", RecordToTFRecordConverter.convert(sr));
                            }
                            final String key = sr.getRecord().get(keyField).toString();
                            return KV.of(key, RecordToTFRecordConverter.convert(sr));
                        })
                        .fromQuery(options.getQuery())
                        .withOutputTag(tagOutput)
                        .withParallelNum(options.getParallelNum())
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), ByteArrayCoder.of()))).get(tagOutput)
                .apply("WriteTFRecord", FileIO.<String, KV<String, byte[]>>writeDynamic()
                        .by(kv -> kv.getKey())
                        .to(outputVP)
                        .withNaming(key -> FileIO.Write.defaultNaming(outputVP.get() + key, ".tfrecord"))
                        .via(Contextful.fn(kv -> kv.getValue()), TFRecordIO.sink())
                        .withCompression(Compression.GZIP)
                        .withDestinationCoder(StringUtf8Coder.of()));

        pipeline.run();
    }
}
