package net.orfeon.cloud.dataflow.templates;

import net.orfeon.cloud.dataflow.dofns.StructToMutationDoFn;
import net.orfeon.cloud.dataflow.transforms.AvroToStructTransform;
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

        @Description("Spanner insert policy. INSERT, UPDATE, REPLACE, or INSERT_OR_UPDATE.")
        @Default.String("INSERT_OR_UPDATE")
        ValueProvider<String> getMutationOp();
        void setMutationOp(ValueProvider<String> mutationOp);

    }

    public static void main(String[] args) {

        AvroToSpannerPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(AvroToSpannerPipelineOption.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadAvroFile", new AvroToStructTransform(options.getInput()))
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(options.getTable(), options.getMutationOp())))
                .apply("StoreSpanner", SpannerIO.write()
                        .withProjectId(options.getProjectId())
                        .withInstanceId(options.getInstanceId())
                        .withDatabaseId(options.getDatabaseId()));

        pipeline.run();
    }
}
