package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.dofns.SpannerTablePrepareDoFn;
import net.orfeon.cloud.dataflow.transforms.BigQueryDirectIO;
import net.orfeon.cloud.dataflow.transforms.StructToAvroTransform;
import net.orfeon.cloud.dataflow.dofns.StructToMutationDoFn;
import net.orfeon.cloud.dataflow.util.converter.MutationToStructConverter;
import net.orfeon.cloud.dataflow.util.converter.RecordToStructConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;


public class BigQueryToSpanner {

    public interface BigQueryToSpannerPipelineOption extends PipelineOptions {

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("Project id spanner for store belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id for store")
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id for store")
        ValueProvider<String> getDatabaseId();
        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("Spanner table name to store query result")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> table);

        @Description("Spanner table name to store query result")
        ValueProvider<String> getOutputError();
        void setOutputError(ValueProvider<String> outputError);

        @Description("Spanner table name to store query result")
        @Default.String("INSERT_OR_UPDATE")
        ValueProvider<String> getMutationOp();
        void setMutationOp(ValueProvider<String> mutationOp);

        @Description("PrimaryKeyFields")
        ValueProvider<String> getPrimaryKeyFields();
        void setPrimaryKeyFields(ValueProvider<String> primaryKeyFields);

        @Description("Field key to separate output path")
        ValueProvider<String> getFieldKey();
        void setFieldKey(ValueProvider<String> fieldKey);

        @Description("Use snappy or default codec")
        @Default.Boolean(true)
        ValueProvider<Boolean> getUseSnappy();
        void setUseSnappy(ValueProvider<Boolean> useSnappy);

        @Description("Parallel read num to request BigQuery Storage API.")
        @Default.Integer(0)
        ValueProvider<Integer> getParallelNum();
        void setParallelNum(ValueProvider<Integer> parallelNum);

    }

    public static void main(final String[] args) {

        final BigQueryToSpannerPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(BigQueryToSpannerPipelineOption.class);

        final Pipeline pipeline = Pipeline.create(options);
        final TupleTag<Struct> tagOutput = new TupleTag<Struct>(){};
        final PCollectionTuple tuple = pipeline
                .apply("QueryBigQuery", BigQueryDirectIO.read(RecordToStructConverter::convert)
                        .fromQuery(options.getQuery())
                        .withOutputTag(tagOutput)
                        .withParallelNum(options.getParallelNum())
                        .withCoder(AvroCoder.of(Struct.class)));

        final PCollection<Struct> dummyStruct = tuple.get(BigQueryDirectIO.tagTableSchema)
                .apply("PrepareSpannerTable", ParDo.of(new SpannerTablePrepareDoFn(
                        options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getTable(), options.getPrimaryKeyFields())));

        final PCollection<Struct> structs = PCollectionList.of(tuple.get(tagOutput)).and(dummyStruct)
                .apply("Flatten", Flatten.pCollections());

        final SpannerWriteResult result = structs
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(options.getTable(), options.getMutationOp())))
                .apply("StoreSpanner", SpannerIO.write()
                        .withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES)
                        .withProjectId(options.getProjectId())
                        .withInstanceId(options.getInstanceId())
                        .withDatabaseId(options.getDatabaseId()));

        result.getFailedMutations()
                .apply("ErrorMutationToStruct", FlatMapElements
                        .into(TypeDescriptor.of(Struct.class))
                        .via(r -> MutationToStructConverter.convert(r)))
                .apply("StoreErrorStorage", new StructToAvroTransform(options.getOutputError(), options.getFieldKey(), options.getUseSnappy()));

        final PCollectionView<String> tableView = tuple.get(BigQueryDirectIO.tagTable)
                .apply("TableAsView", View.asSingleton());

        result.getFailedMutations()
                .apply("CountFailedMutation", Count.globally())
                .apply("DeleteTempDataset", ParDo.of(new DoFn<Long, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        final String table = c.sideInput(tableView);
                        final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
                        bigquery.getTable(TableId.of(table, table)).delete();
                        bigquery.getDataset(DatasetId.of(table)).delete(BigQuery.DatasetDeleteOption.deleteContents());
                    }
                }).withSideInputs(tableView));

        pipeline.run();
    }

}

