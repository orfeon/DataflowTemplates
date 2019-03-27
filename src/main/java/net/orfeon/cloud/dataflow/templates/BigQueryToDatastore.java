package net.orfeon.cloud.dataflow.templates;

import com.google.datastore.v1.Entity;
import net.orfeon.cloud.dataflow.transforms.BigQueryDirectIO;
import net.orfeon.cloud.dataflow.util.converter.RecordToEntityConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;


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

        @Description("Field names to exclude from index.")
        ValueProvider<String> getExcludeFromIndexFields();
        void setExcludeFromIndexFields(ValueProvider<String> excludeFromIndexFields);

        @Description("Parallel num.")
        @Default.Integer(0)
        ValueProvider<Integer> getParallelNum();
        void setParallelNum(ValueProvider<Integer> parallelNum);
    }

    public static void main(final String[] args) {

        final BigQueryToDatastorePipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(BigQueryToDatastorePipelineOption.class);

        final Pipeline pipeline = Pipeline.create(options);

        final TupleTag<Entity> tagOutput = new TupleTag<Entity>(){};
        final ValueProvider<String> kind = options.getKind();
        final ValueProvider<String> keyField = options.getKeyField();
        final ValueProvider<String> excludeFromIndexFields = options.getExcludeFromIndexFields();

        final PCollectionTuple tuple = pipeline
                .apply("QueryBigQuery", BigQueryDirectIO.read(r -> RecordToEntityConverter.convert(r, kind, keyField, excludeFromIndexFields))
                        .fromQuery(options.getQuery())
                        .withOutputTag(tagOutput)
                        .withParallelNum(options.getParallelNum())
                        .withCoder(SerializableCoder.of(Entity.class)));

        // For extract mode. This mode requires user deploy to set tempLocation, but higher throughput.
        /*
        pipeline.apply("QueryBigQuery", BigQueryIO
                        .read(r -> RecordToEntityConverter.convert(r, kind, keyField, excludeFromIndexFields))
                        .fromQuery(options.getQuery())
                        .usingStandardSql()
                        .withQueryPriority(BigQueryIO.TypedRead.QueryPriority.INTERACTIVE)
                        .withTemplateCompatibility()
                        .withCoder(SerializableCoder.of(Entity.class))
                        .withoutValidation())
        */

        tuple.get(tagOutput)
                .apply("AsKeyValue", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(Entity.class)))
                        .via((entity -> KV.of(entity.getKey().toString(), entity))))
                .apply("GroupByKey", GroupByKey.create())
                .apply("Flatten", MapElements.into(TypeDescriptor.of(Entity.class))
                        .via(entity -> entity.getValue().iterator().next()))
                .apply("StoreDatastore", DatastoreIO.v1().write().withProjectId(options.getProjectId()));

        pipeline.run();
    }

}
