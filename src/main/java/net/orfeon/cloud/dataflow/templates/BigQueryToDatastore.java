package net.orfeon.cloud.dataflow.templates;

import com.google.datastore.v1.Entity;
import net.orfeon.cloud.dataflow.util.converter.RecordToEntityConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.*;

public class BigQueryToDatastore {

    public interface BigQueryToDatastorePipelineOption extends PipelineOptions {

        @Description("SQL Query text to read records from BigQuery")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("Project ID that datastore is belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> output);

        @Description("Cloud Datastore target kind name to store.")
        ValueProvider<String> getKind();
        void setKind(ValueProvider<String> databaseId);

        @Description("Unique field name in query results from BigQuery.")
        ValueProvider<String> getKeyField();
        void setKeyField(ValueProvider<String> fieldKey);

    }

    public static void main(final String[] args) {

        final BigQueryToDatastorePipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(BigQueryToDatastorePipelineOption.class);

        final Pipeline pipeline = Pipeline.create(options);

        final ValueProvider<String> kind = options.getKind();
        final ValueProvider<String> keyField = options.getKeyField();

        pipeline.apply("QueryBigQuery", BigQueryIO.read(r -> RecordToEntityConverter.convert(r, kind, keyField))
                        .fromQuery(options.getQuery())
                        .usingStandardSql()
                        .withQueryPriority(BigQueryIO.TypedRead.QueryPriority.INTERACTIVE)
                        .withTemplateCompatibility()
                        .withCoder(SerializableCoder.of(Entity.class))
                        .withoutValidation())
                .apply("StoreDatastore", DatastoreIO.v1().write().withProjectId(options.getProjectId()));

        pipeline.run();
    }

}
