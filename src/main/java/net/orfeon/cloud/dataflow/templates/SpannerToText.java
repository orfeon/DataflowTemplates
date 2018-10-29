package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.spanner.SpannerSimpleIO;
import net.orfeon.cloud.dataflow.spanner.StructToCsvDoFn;
import net.orfeon.cloud.dataflow.spanner.StructToJsonDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class SpannerToText {

    public interface SpannerToTextPipelineOption extends PipelineOptions {

        @Description("Format type, choose csv or json")
        @Default.String("json")
        @Validation.Required
        String getType();
        void setType(String type);

        @Description("Use single process query or parallel process query.")
        @Default.Boolean(false)
        @Validation.Required
        Boolean getSingle();
        void setSingle(Boolean type);

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

        PTransform<PBegin, PCollection<Struct>> spannerIO = options.getSingle() ?
                SpannerSimpleIO.readSingle(options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getQuery(), options.getTimestampBound()) :
                SpannerSimpleIO.read(options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getQuery(), options.getTimestampBound());

        DoFn<Struct, String> textConverter = "json".equals(options.getType().toLowerCase()) ? new StructToJsonDoFn() : new StructToCsvDoFn();
        String suffix = "json".equals(options.getType().toLowerCase()) ? ".json" : ".csv";

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("ReadSpanner", spannerIO)
                .apply("ConvertLine", ParDo.of(textConverter))
                .apply("StoreStorage", TextIO.write().to(options.getOutput()).withSuffix(suffix));

        pipeline.run();
    }
}
