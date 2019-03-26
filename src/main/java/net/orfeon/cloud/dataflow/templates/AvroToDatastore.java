package net.orfeon.cloud.dataflow.templates;

import com.google.datastore.v1.Entity;
import net.orfeon.cloud.dataflow.util.converter.RecordToEntityConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

public class AvroToDatastore {

    public interface AvroToDatastorePipelineOption extends PipelineOptions {

        @Description("SQL Query text to read records from BigQuery")
        ValueProvider<String> getInput();
        void setInput(ValueProvider<String> input);

        @Description("Project ID that datastore is belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> output);

        @Description("Cloud Datastore target kind name to store.")
        ValueProvider<String> getKind();
        void setKind(ValueProvider<String> databaseId);

        @Description("Unique field name in query results from BigQuery.")
        ValueProvider<String> getKeyField();
        void setKeyField(ValueProvider<String> fieldKey);

        @Description("Field names to exclude from index.")
        ValueProvider<String> getExcludeFromIndexFields();
        void setExcludeFromIndexFields(ValueProvider<String> excludeFromIndexFields);

    }

    public static void main(final String[] args) {

        final AvroToDatastorePipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(AvroToDatastorePipelineOption.class);

        final Pipeline pipeline = Pipeline.create(options);

        final ValueProvider<String> kind = options.getKind();
        final ValueProvider<String> keyField = options.getKeyField();
        final ValueProvider<String> excludeFromIndexFields = options.getExcludeFromIndexFields();

        pipeline.apply("ReadAvro", AvroIO
                    .parseGenericRecords(r -> RecordToEntityConverter.convert(r, kind, keyField, excludeFromIndexFields))
                    .withCoder(SerializableCoder.of(Entity.class))
                    .from(options.getInput()))
                .apply("StoreDatastore", DatastoreIO.v1().write().withProjectId(options.getProjectId()));

        pipeline.run();
    }
}
