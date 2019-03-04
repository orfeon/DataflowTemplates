package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.spanner.Mutation;
import net.orfeon.cloud.dataflow.dofns.StructToMutationDoFn;
import net.orfeon.cloud.dataflow.transforms.SpannerQueryIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;


public class SpannerToSpannerDelete {

    public interface SpannerToSpannerDeletePipelineOption extends PipelineOptions {

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

        @Description("Spanner table name to delete records")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> table);

        @Description("Key fields in query results. If composite key case, set comma-separated fields in key sequence.")
        ValueProvider<String> getKeyFields();
        void setKeyFields(ValueProvider<String> keyFields);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);
    }

    public static void main(String[] args) {

        SpannerToSpannerDeletePipelineOption options = PipelineOptionsFactory.fromArgs(args).as(SpannerToSpannerDeletePipelineOption.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("QuerySpanner", SpannerQueryIO.read(
                        options.getProjectId(),
                        options.getInstanceId(),
                        options.getDatabaseId(),
                        options.getQuery(),
                        options.getTimestampBound()))
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(
                        options.getTable(),
                        ValueProvider.StaticValueProvider.of(Mutation.Op.DELETE.name()),
                        options.getKeyFields())))
                .apply("DeleteMutation", SpannerIO.write()
                        .withProjectId(options.getProjectId())
                        .withInstanceId(options.getInstanceId())
                        .withDatabaseId(options.getDatabaseId()));

        pipeline.run();
    }

}
