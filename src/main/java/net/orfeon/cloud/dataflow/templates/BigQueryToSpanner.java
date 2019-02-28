package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.transforms.StructToAvroTransform;
import net.orfeon.cloud.dataflow.dofns.StructToMutationDoFn;
import net.orfeon.cloud.dataflow.util.converter.MutationToStructConverter;
import net.orfeon.cloud.dataflow.util.converter.RecordToStructConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;


public class BigQueryToSpanner {


    public interface BigQueryToSpannerPipelineOption extends PipelineOptions {

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("Project id spanner for store belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> output);

        @Description("Spanner instance id for store")
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id for store")
        ValueProvider<String> getDatabaseId();
        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("Spanner table name to store query result")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> databaseId);

        @Description("Spanner table name to store query result")
        ValueProvider<String> getOutputError();
        void setOutputError(ValueProvider<String> error);

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

        final BigQueryToSpannerPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(BigQueryToSpannerPipelineOption.class);

        final Pipeline pipeline = Pipeline.create(options);
        final SpannerWriteResult result = pipeline
                .apply("QueryBigQuery", BigQueryIO.read(RecordToStructConverter::convert)
                        .fromQuery(options.getQuery())
                        .usingStandardSql()
                        .withQueryPriority(BigQueryIO.TypedRead.QueryPriority.INTERACTIVE)
                        .withTemplateCompatibility()
                        .withCoder(AvroCoder.of(Struct.class))
                        .withoutValidation())
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

