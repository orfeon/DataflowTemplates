package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.spanner.SpannerSimpleIO;
import net.orfeon.cloud.dataflow.spanner.StructToCsvDoFn;
import net.orfeon.cloud.dataflow.spanner.StructToJsonDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class SpannerToText {

    public interface SpannerToTextPipelineOption extends PipelineOptions {

        @Description("Format type, choose csv or json")
        @Default.String("csv")
        @Validation.Required
        String getType();
        void setType(String type);

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

        DoFn<Struct, String> textConverter = "csv".equals(options.getType().toLowerCase()) ? new StructToCsvDoFn() : new StructToJsonDoFn();

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("ReadSpanner", SpannerSimpleIO.read(options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getQuery(), options.getTimestampBound()))
                .apply("ConvertLine", ParDo.of(textConverter))
                .apply("StoreStorage", TextIO.write().to(options.getOutput()));

        pipeline.run();
    }
}
