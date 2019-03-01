package net.orfeon.cloud.dataflow.templates;

import net.orfeon.cloud.dataflow.transforms.DummyToMutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.*;

public class DummyToSpanner {

    public interface DummyToSpannerOption extends PipelineOptions {

        @Description("Project id spanner instance belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id you want to access")
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id you want to access")
        ValueProvider<String> getDatabaseId();
        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("Spanner Table Names and num of dummy record.")
        ValueProvider<String> getTables();
        void setTables(ValueProvider<String> table);

        @Description("Config yaml file. GCS path or yaml text itself.")
        ValueProvider<String> getConfig();
        void setConfig(ValueProvider<String> config);

        @Description("Worker parallel num to generate dummy records.")
        @Default.Long(1)
        ValueProvider<Long> getParallelNum();
        void setParallelNum(ValueProvider<Long> parallelNum);

    }

    public static void main(String[] args) {

        DummyToSpannerOption options = PipelineOptionsFactory.fromArgs(args).as(DummyToSpannerOption.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("GenerateDummy", new DummyToMutation(
                        options.getProjectId(),
                        options.getInstanceId(),
                        options.getDatabaseId(),
                        options.getTables(),
                        options.getConfig(),
                        options.getParallelNum()))
                .apply("StoreSpanner", SpannerIO.write()
                        .withProjectId(options.getProjectId())
                        .withInstanceId(options.getInstanceId())
                        .withDatabaseId(options.getDatabaseId()));

        pipeline.run();
    }


}
