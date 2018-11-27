package net.orfeon.cloud.dataflow.templates;

import com.google.api.services.bigquery.model.TableRow;
import net.orfeon.cloud.dataflow.transforms.SpannerSimpleIO;
import net.orfeon.cloud.dataflow.util.converter.StructToTableRowConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;


public class SpannerToBigQuery {

    public interface SpannerToBigQueryPipelineOption extends PipelineOptions {

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

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("QuerySpanner", SpannerSimpleIO.read(options.getProjectId(), options.getInstanceId(), options.getDatabaseId(), options.getQuery(), options.getTimestampBound()))
                .apply("ConvertTableRow", MapElements.into(TypeDescriptor.of(TableRow.class)).via(StructToTableRowConverter::convert))
                .apply("StoreBigQuery", BigQueryIO.writeTableRows()
                        .to(options.getOutput())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();
    }

}
