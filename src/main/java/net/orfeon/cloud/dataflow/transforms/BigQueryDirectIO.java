package net.orfeon.cloud.dataflow.transforms;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1beta1.*;
import com.google.common.hash.Hashing;
import net.orfeon.cloud.dataflow.util.AvroSchemaUtil;
import net.orfeon.cloud.dataflow.util.converter.RecordToTableRowConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

public class BigQueryDirectIO {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDirectIO.class);

    public static final TupleTag<String> tagTable = new TupleTag<String>(){};
    public static final TupleTag<String> tagTableSchema = new TupleTag<String>(){};

    public static <T extends Serializable> TypedRead<T> read(SerializableFunction<SchemaAndRecord, T> parseFn) {
        final TypedRead<T> read = new TypedRead<>();
        read.parseFn = parseFn;
        return read;
    }

    public static class TypedRead<T> extends PTransform<PBegin, PCollectionTuple> {

        private TupleTag<T> tagOutput;
        private ValueProvider<String> query;
        private ValueProvider<Integer> parallelNum;
        private Coder<T> coder;
        private SerializableFunction<SchemaAndRecord, T> parseFn;

        public TypedRead<T> fromQuery(ValueProvider<String> query) {
            this.query = query;
            return this;
        }

        public TypedRead<T> withParallelNum(ValueProvider<Integer> parallelNum) {
            this.parallelNum = parallelNum;
            return this;
        }

        public TypedRead<T> withCoder(Coder<T> coder) {
            this.coder = coder;
            return this;
        }

        public TypedRead<T> withOutputTag(TupleTag<T> tagOutput) {
            this.tagOutput = tagOutput;
            return this;
        }

        @Override
        public PCollectionTuple expand(PBegin input) {
            final PCollectionTuple tuple = input.getPipeline()
                    .apply("SupplyQuery", Create.ofProvider(query, StringUtf8Coder.of()))
                    .apply("ExecuteQuery", ParDo.of(new QueryExecuteDoFn())
                            .withOutputTags(QueryExecuteDoFn.tagTable, TupleTagList.of(QueryExecuteDoFn.tagAvroSchema)));

            final PCollectionView<String> schemaView = tuple.get(QueryExecuteDoFn.tagAvroSchema).apply(View.asSingleton());

            final PCollection<T> res = tuple.get(QueryExecuteDoFn.tagTable)
                    .apply("DirectRead", ParDo.of(new DirectReadDoFn(parallelNum)))
                    .apply("GroupByKey", GroupByKey.create())
                    .apply("ReadParallel", (ParDo.SingleOutput<KV<Integer, Iterable<Storage.Stream>>, T>)
                            ParDo.of(new StreamReadDoFn(parseFn, schemaView)).withSideInputs(schemaView)).setCoder(coder);

            return PCollectionTuple.of(this.tagOutput, res)
                    .and(tagTableSchema, tuple.get(QueryExecuteDoFn.tagAvroSchema))
                    .and(tagTable, tuple.get(QueryExecuteDoFn.tagTable));
        }
    }

    public static class QueryExecuteDoFn extends DoFn<String, String> {

        public static final TupleTag<String> tagTable  = new TupleTag<String>(){};
        public static final TupleTag<String> tagAvroSchema = new TupleTag<String>(){};
        public static final TupleTag<TableSchema> tagTableSchema = new TupleTag<TableSchema>(){};

        @ProcessElement
        public void processElement(ProcessContext c) throws InterruptedException {
            //final String projectId = c.getPipelineOptions().as(DataflowPipelineOptions.class).getProject();
            final String randomName = Hashing.sha1().hashLong(new Random().nextLong()).toString();
            final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            final DatasetInfo datasetInfo = DatasetInfo.newBuilder(randomName).build();
            bigquery.create(datasetInfo);
            final TableId tableId = TableId.of(randomName, randomName);
            //
            final QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(c.element())
                    .setDestinationTable(tableId)
                    .setUseLegacySql(false)
                    .setAllowLargeResults(true)
                    .setPriority(QueryJobConfiguration.Priority.INTERACTIVE)
                    .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                    .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                    .build();
            final JobInfo jobInfo = JobInfo.of(queryConfig);

            Job job = bigquery.create(jobInfo);
            int count = 0;
            while(!job.isDone()) {
                Thread.sleep(1000 * 5);
                job = bigquery.getJob(job.getJobId());
                LOG.info(String.format("Waiting query job complete %d sec.", count += 5));
                if(count > 3600) {
                    throw new RuntimeException("Query takes too long time(over 1 hour.)");
                }
            }
            if(job.getStatus().getError() != null) {
                LOG.error(job.getStatus().getError().getMessage());
                final BigQueryError error = job.getStatus().getError();
                throw new RuntimeException(error.getMessage() + ". cause: " + error.getReason() + ". location: " + error.getLocation());
            }
            final Table table = bigquery.getTable(tableId);
            final com.google.cloud.bigquery.Schema tableSchema = table.getDefinition().getSchema();
            final Schema avroSchema = AvroSchemaUtil.convertSchema(tableSchema);
            c.output(randomName);
            c.output(tagAvroSchema, avroSchema.toString());
            LOG.info(avroSchema.toString());
        }
    }


    public static class DirectReadDoFn extends DoFn<String, KV<Integer, Storage.Stream>> {

        private final ValueProvider<Integer> parallelNum;

        public DirectReadDoFn(ValueProvider<Integer> parallelNum) {
            this.parallelNum = parallelNum;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            final String projectId = c.getPipelineOptions().as(DataflowPipelineOptions.class).getProject();
            final BigQueryStorageSettings settings = BigQueryStorageSettings.newBuilder().build();
            final BigQueryStorageClient client = BigQueryStorageClient.create(settings);
                final TableReferenceProto.TableReference tableReference = TableReferenceProto.TableReference.newBuilder()
                        .setProjectId(projectId)
                        .setDatasetId(c.element())
                        .setTableId(c.element())
                        .build();

                final Storage.CreateReadSessionRequest.Builder builder = Storage.CreateReadSessionRequest.newBuilder()
                        .setParent(String.format("projects/%s", projectId))
                        .setTableReference(tableReference)
                        .setRequestedStreams(this.parallelNum.get())
                        .setFormat(Storage.DataFormat.AVRO);

                final Storage.ReadSession response = client.createReadSession(builder.build());
                LOG.info(String.format("StreamCount: %d", response.getStreamsCount()));
                for(int i=0; i<response.getStreamsCount(); i++) {
                    c.output(KV.of(i, response.getStreams(i)));
                }
            }

    }

    public static class StreamReadDoFn<T extends Serializable> extends DoFn<KV<Integer, Iterable<Storage.Stream>>, T> {

        private final PCollectionView<String> schemaView;
        private final SerializableFunction<SchemaAndRecord, T> parseFn;

        private GenericRecord record = null;
        private BinaryDecoder decoder = null;

        public StreamReadDoFn(SerializableFunction<SchemaAndRecord, T> parseFn, PCollectionView<String> schemaView) {
            this.parseFn = parseFn;
            this.schemaView = schemaView;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            final String schemaJsonString = c.sideInput(schemaView);
            final Schema schema = new Schema.Parser().parse(schemaJsonString);
            final TableSchema tableSchema = RecordToTableRowConverter.convertTableSchema(schema);
            final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            final Storage.Stream stream = c.element().getValue().iterator().next();
            LOG.info(String.format("Start to read %d rows.", stream.getRowCount()));
            try(final BigQueryStorageClient client = BigQueryStorageClient.create()) {

                final Storage.ReadRowsRequest request = Storage.ReadRowsRequest.newBuilder()
                                .setReadPosition(Storage.StreamPosition.newBuilder()
                                        .setStream(stream)
                                        .build()).build();
                for(final Storage.ReadRowsResponse response : client.readRowsCallable().call(request)) {
                    decoder = DecoderFactory.get().binaryDecoder(
                            response.getAvroRows().getSerializedBinaryRows().toByteArray(), decoder);
                    while(!decoder.isEnd()) {
                        record = datumReader.read(record, decoder);
                        final T t = this.parseFn.apply(new SchemaAndRecord(record, tableSchema));
                        c.output(t);
                    }
                }
            }
        }

    }

}
