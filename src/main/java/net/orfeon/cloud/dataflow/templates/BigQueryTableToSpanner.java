package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.bigquery.*;
import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.dofns.SpannerTablePrepareDoFn;
import net.orfeon.cloud.dataflow.dofns.StructToMutationDoFn;
import net.orfeon.cloud.dataflow.transforms.StructToAvroTransform;
import net.orfeon.cloud.dataflow.util.AvroSchemaUtil;
import net.orfeon.cloud.dataflow.util.converter.MutationToStructConverter;
import net.orfeon.cloud.dataflow.util.converter.RecordToStructConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class BigQueryTableToSpanner {

    public interface BigQueryTableToSpannerPipelineOption extends PipelineOptions {

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getInputTable();
        void setInputTable(ValueProvider<String> inputTable);

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

        @Description("PrimaryKeyFields")
        ValueProvider<String> getPrimaryKeyFields();
        void setPrimaryKeyFields(ValueProvider<String> primaryKeyFields);

        @Description("Spanner table name to store query result")
        ValueProvider<String> getOutputError();
        void setOutputError(ValueProvider<String> outputError);

        @Description("Spanner table name to store query result")
        @Default.String("INSERT_OR_UPDATE")
        ValueProvider<String> getMutationOp();
        void setMutationOp(ValueProvider<String> mutationOp);

        @Description("Field key to separate output path")
        ValueProvider<String> getFieldKey();
        void setFieldKey(ValueProvider<String> fieldKey);

        @Description("Use snappy or default codec")
        @Default.Boolean(true)
        ValueProvider<Boolean> getUseSnappy();
        void setUseSnappy(ValueProvider<Boolean> useSnappy);

    }

    public static void main(final String[] args) {

        final BigQueryTableToSpannerPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(BigQueryTableToSpannerPipelineOption.class);

        final Pipeline pipeline = Pipeline.create(options);
        final PCollection<Struct> struct = pipeline
                .apply("ReadBigQueryTable", BigQueryIO.read(RecordToStructConverter::convert)
                        .from(options.getInputTable())
                        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                        .withTemplateCompatibility()
                        .withoutValidation()
                        .withCoder(AvroCoder.of(Struct.class)));

        final PCollection<Struct> dummyStruct = pipeline
                .apply("SupplyBigQueryTable", Create.ofProvider(options.getInputTable(), StringUtf8Coder.of()))
                .apply("ReadSchema", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
                        final String[] source = c.element().split("\\.");
                        final TableId tableId = source.length == 3 ? TableId.of(source[0], source[1], source[2]) : TableId.of(source[0], source[1]);
                        final Table table = bigquery.getTable(tableId);
                        final com.google.cloud.bigquery.Schema tableSchema = table.getDefinition().getSchema();
                        final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(tableSchema);
                        c.output(avroSchema.toString());
                    }
                }))
                .apply("PrepareSpannerTable", ParDo.of(new SpannerTablePrepareDoFn(
                        options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getTable(), options.getPrimaryKeyFields())));

        final PCollection<Struct> structs = PCollectionList.of(struct).and(dummyStruct)
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

        pipeline.run();

    }
}
