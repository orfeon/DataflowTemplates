package net.orfeon.cloud.dataflow.templates;

import net.orfeon.cloud.dataflow.spanner.SpannerSimpleIO;
import net.orfeon.cloud.dataflow.spanner.StructToTextDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;

public class SpannerToText {

    public interface SpannerToTextPipelineOption extends PipelineOptions {

        @Description("Format type, choose csv or json. default is json.")
        ValueProvider<String> getType();
        void setType(ValueProvider<String> type);

        @Description("Project id spanner instance belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id you want to access")
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id you want to access")
        ValueProvider<String> getDatabaseId();
        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("GCS path to output. prefix must start with gs://")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> output);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);
    }

    public static void main(String[] args) {

        SpannerToTextPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(SpannerToTextPipelineOption.class);

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("QuerySpanner", SpannerSimpleIO.read(options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getQuery(), options.getTimestampBound()))
                .apply("ConvertLine", ParDo.of(new StructToTextDoFn(options.getType())))
                .apply("StoreStorage", TextIO.write().to(options.getOutput()).withSuffix(".txt"));

        pipeline.run();
    }
}
