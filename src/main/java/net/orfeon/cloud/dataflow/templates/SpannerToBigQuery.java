package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.spanner.SpannerSimpleIO;
import net.orfeon.cloud.dataflow.spanner.StructToTableRowDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class SpannerToBigQuery {

    public interface SpannerToBigQueryPipelineOption extends PipelineOptions {

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

        @Description("Destination BigQuery table. format {dataset}.{table}")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> output);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);
    }

    public static void main(String[] args) {

        SpannerToBigQueryPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(SpannerToBigQueryPipelineOption.class);

        PTransform<PBegin, PCollection<Struct>> spannerIO = options.getSingle() ?
                SpannerSimpleIO.readSingle(options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getQuery(), options.getTimestampBound()) :
                SpannerSimpleIO.read(options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getQuery(), options.getTimestampBound());

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("QuerySpanner", spannerIO)
                .apply("ConvertTableRow", ParDo.of(new StructToTableRowDoFn()))
                .apply("StoreBigQuery", BigQueryIO.writeTableRows()
                        .to(options.getOutput())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();
    }

}
