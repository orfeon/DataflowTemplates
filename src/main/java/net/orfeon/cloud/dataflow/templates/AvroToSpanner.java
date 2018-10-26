package net.orfeon.cloud.dataflow.templates;

import net.orfeon.cloud.dataflow.spanner.StructToMutationDoFn;
import net.orfeon.cloud.dataflow.storage.AvroToStructTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;

public class AvroToSpanner {

    public interface AvroToSpannerPipelineOption extends PipelineOptions {

        @Description("Project id spanner instance belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id you want to access")
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id you want to access")
        ValueProvider<String> getDatabaseId();
        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("Spanner Database id you want to access")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> table);

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getInput();
        void setInput(ValueProvider<String> input);

    }

    public static void main(String[] args) {

        AvroToSpannerPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(AvroToSpannerPipelineOption.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadAvroFile", new AvroToStructTransform(options.getInput()))
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(options.getTable())))
                .apply("StoreSpanner", SpannerIO.write()
                        .withProjectId(options.getProjectId())
                        .withInstanceId(options.getInstanceId())
                        .withDatabaseId(options.getDatabaseId()));

        pipeline.run();
    }
}
