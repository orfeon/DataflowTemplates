package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.spanner.SpannerSimpleIO;
import net.orfeon.cloud.dataflow.spanner.StructToAvroTransform;
import net.orfeon.cloud.dataflow.spanner.StructToMutationDoFn;
import net.orfeon.cloud.dataflow.spanner.StructUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;


public class SpannerToSpanner {

    public interface SpannerToSpannerPipelineOption extends PipelineOptions {

        @Description("Project id spanner for query belong to")
        ValueProvider<String> getInputProjectId();
        void setInputProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id for query")
        ValueProvider<String> getInputInstanceId();
        void setInputInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id for query")
        ValueProvider<String> getInputDatabaseId();
        void setInputDatabaseId(ValueProvider<String> databaseId);

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("Project id spanner for store belong to")
        ValueProvider<String> getOutputProjectId();
        void setOutputProjectId(ValueProvider<String> output);

        @Description("Spanner instance id for store")
        ValueProvider<String> getOutputInstanceId();
        void setOutputInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id for store")
        ValueProvider<String> getOutputDatabaseId();
        void setOutputDatabaseId(ValueProvider<String> databaseId);

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

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);

    }

    public static void main(final String[] args) {

        final SpannerToSpannerPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(SpannerToSpannerPipelineOption.class);

        final Pipeline pipeline = Pipeline.create(options);
        final SpannerWriteResult result = pipeline
                .apply("QuerySpanner", SpannerSimpleIO.read(options.getInputProjectId(), options.getInputInstanceId(), options.getInputDatabaseId(), options.getQuery(), options.getTimestampBound()))
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(options.getTable(), options.getMutationOp())))
                .apply("StoreSpanner", SpannerIO.write()
                        .withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES)
                        .withProjectId(options.getOutputProjectId())
                        .withInstanceId(options.getOutputInstanceId())
                        .withDatabaseId(options.getOutputDatabaseId()));

        result.getFailedMutations()
                .apply("ErrorMutationToStruct", FlatMapElements.into(TypeDescriptor.of(Struct.class)).via(StructUtil::from))
                .apply("StoreErrorStorage", new StructToAvroTransform(options.getOutputError(), options.getFieldKey(), options.getUseSnappy()));

        pipeline.run();
    }

}