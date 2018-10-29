package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.spanner.SpannerSimpleIO;
import net.orfeon.cloud.dataflow.spanner.StructToAvroTransform;
import net.orfeon.cloud.dataflow.spanner.StructToMutationDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class SpannerToSpanner {

    public interface SpannerToSpannerPipelineOption extends PipelineOptions {

        @Description("Use single process query or parallel process query.")
        @Default.Boolean(true)
        @Validation.Required
        Boolean getSingle();
        void setSingle(Boolean type);

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
        ValueProvider<String> getOutputTable();
        void setOutputTable(ValueProvider<String> databaseId);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);
    }

    public static void main(String[] args) {

        SpannerToSpannerPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(SpannerToSpannerPipelineOption.class);

        PTransform<PBegin, PCollection<Struct>> spannerIO = options.getSingle() ?
                SpannerSimpleIO.readSingle(options.getInputProjectId(), options.getInputInstanceId(), options.getInputDatabaseId(), options.getQuery(), options.getTimestampBound()) :
                SpannerSimpleIO.read(options.getInputProjectId(), options.getInputInstanceId(), options.getInputDatabaseId(), options.getQuery(), options.getTimestampBound());

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("QuerySpanner", spannerIO)
                .apply("ConvertMutation", ParDo.of(new StructToMutationDoFn(options.getOutputTable())))
                .apply("StoreSpanner", SpannerIO.write()
                        .withProjectId(options.getOutputProjectId())
                        .withInstanceId(options.getOutputInstanceId())
                        .withDatabaseId(options.getOutputDatabaseId()));

        pipeline.run();
    }

}
