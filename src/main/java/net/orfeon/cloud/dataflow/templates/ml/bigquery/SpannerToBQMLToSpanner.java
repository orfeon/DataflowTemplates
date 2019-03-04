package net.orfeon.cloud.dataflow.templates.ml.bigquery;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.dofns.StructToMutationDoFn;
import net.orfeon.cloud.dataflow.transforms.SpannerQueryIO;
import net.orfeon.cloud.dataflow.transforms.StructToAvroTransform;
import net.orfeon.cloud.dataflow.util.converter.MutationToStructConverter;
import net.orfeon.cloud.dataflow.util.mlmodel.BQMLModel;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

public class SpannerToBQMLToSpanner {

    private static final String BQML_WEIGHT_SQL =
            "SELECT a.*, b.* EXCEPT(input)\n" +
            "FROM ML.WEIGHTS(MODEL `%s`) a\n" +
            "LEFT OUTER JOIN ML.FEATURE_INFO(MODEL `%s`) b\n" +
            "ON a.processed_input = b.input";

    public interface SpannerToBQMLToSpannerPipelineOption extends PipelineOptions {

        @Description("BQML Model. format: {project_id}.{dataset}.{model_name}")
        ValueProvider<String> getModel();
        void setModel(ValueProvider<String> model);

        @Description("BQML Model type. `linear_reg` or `logistic_reg`")
        ValueProvider<String> getModelType();
        void setModelType(ValueProvider<String> modelType);

        @Description("Unique key field name in spanner query result records.")
        ValueProvider<String> getKeyFieldName();
        void setKeyFieldName(ValueProvider<String> keyFieldName);

        @Description("Project id spanner for query belong to")
        ValueProvider<String> getInputProjectId();
        void setInputProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id for query")
        ValueProvider<String> getInputInstanceId();
        void setInputInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id for query")
        ValueProvider<String> getInputDatabaseId();
        void setInputDatabaseId(ValueProvider<String> databaseId);

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("Project id spanner for store belong to")
        ValueProvider<String> getOutputProjectId();
        void setOutputProjectId(ValueProvider<String> output);

        @Description("Spanner instance id for store")
        ValueProvider<String> getOutputInstanceId();
        void setOutputInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id for store")
        ValueProvider<String> getOutputDatabaseId();
        void setOutputDatabaseId(ValueProvider<String> databaseId);

        @Description("Spanner table name to store query result")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> databaseId);

        @Description("Spanner table name to store query result")
        ValueProvider<String> getOutputError();
        void setOutputError(ValueProvider<String> error);

        @Description("Spanner table name to store query result")
        @Default.String("INSERT_OR_UPDATE")
        ValueProvider<String> getMutationOp();
        void setMutationOp(ValueProvider<String> mutationOp);

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

    public static void main(final String[] args) {

        final SpannerToBQMLToSpannerPipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(SpannerToBQMLToSpannerPipelineOption.class);

        final Pipeline pipeline = Pipeline.create(options);

        final PCollectionView<BQMLModel> estimatorView = pipeline
                .apply("LoadBQMLWeightInfo", BigQueryIO.readTableRows()
                    .fromQuery(ValueProvider.NestedValueProvider
                        .of(options.getModel(), m -> String.format(BQML_WEIGHT_SQL, m, m)))
                    .withoutValidation()
                    .usingStandardSql()
                    .withTemplateCompatibility())
                .apply("CombineToBQMLModel", Combine.globally(new BQMLLoadCombineFn()))
                .apply("AsBQMLModelView", View.asSingleton());

        final SpannerWriteResult result = pipeline
                .apply("QuerySpanner", SpannerQueryIO.read(
                        options.getInputProjectId(),
                        options.getInputInstanceId(),
                        options.getInputDatabaseId(),
                        options.getQuery(),
                        options.getTimestampBound()))
                .apply("Predict", ParDo.of(new BQMLPredictDoFn(
                        options.getModelType(),
                        options.getKeyFieldName(),
                        estimatorView))
                    .withSideInputs(estimatorView))
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(options.getTable(), options.getMutationOp())))
                .apply("StoreSpanner", SpannerIO.write()
                    .withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES)
                    .withProjectId(options.getOutputProjectId())
                    .withInstanceId(options.getOutputInstanceId())
                    .withDatabaseId(options.getOutputDatabaseId()));

        result.getFailedMutations()
                .apply("ErrorMutationToStruct", FlatMapElements
                        .into(TypeDescriptor.of(Struct.class))
                        .via(MutationToStructConverter::convert))
                .apply("StoreErrorStorage", new StructToAvroTransform(options.getOutputError(), options.getFieldKey(), options.getUseSnappy()));

        pipeline.run();
    }
}
