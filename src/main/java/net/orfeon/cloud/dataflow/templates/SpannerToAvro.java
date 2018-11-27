package net.orfeon.cloud.dataflow.templates;

import net.orfeon.cloud.dataflow.transforms.SpannerSimpleIO;
import net.orfeon.cloud.dataflow.transforms.StructToAvroTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.*;


public class SpannerToAvro {

    public interface SpannerToAvroPipelineOption extends PipelineOptions {

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

    public static void main(String[] args) {

        SpannerToAvroPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(SpannerToAvroPipelineOption.class);

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("QuerySpanner", SpannerSimpleIO.read(options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getQuery(), options.getTimestampBound()))
                .apply("StoreGCSAvro", new StructToAvroTransform(options.getOutput(), options.getFieldKey(), options.getUseSnappy()));

        pipeline.run();
    }

}
