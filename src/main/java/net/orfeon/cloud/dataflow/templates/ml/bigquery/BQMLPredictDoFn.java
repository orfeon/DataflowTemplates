package net.orfeon.cloud.dataflow.templates.ml.bigquery;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import net.orfeon.cloud.dataflow.util.mlmodel.BQMLModel;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class BQMLPredictDoFn extends DoFn<Struct, Struct> {

    private static final String MODEL_TYPE_LINEAR_REG = "linear_reg";
    private static final String MODEL_TYPE_LOGISTIC_REG = "logistic_reg";
    private static final String FIELD_NAME_SCORE = "score";

    private String modelType;
    private String keyFieldName;
    private final ValueProvider<String> modelTypeVP;
    private final ValueProvider<String> keyFieldNameVP;
    private final PCollectionView<BQMLModel> estimatorView;

    public BQMLPredictDoFn(ValueProvider<String> modelType, ValueProvider<String> keyFieldName, PCollectionView<BQMLModel> estimatorView) {
        this.modelTypeVP = modelType;
        this.keyFieldNameVP = keyFieldName;
        this.estimatorView = estimatorView;
    }

    @Setup
    public void setup() {
        this.modelType = this.modelTypeVP.get();
        this.keyFieldName = this.keyFieldNameVP.get();
        if(!this.modelType.equals(MODEL_TYPE_LINEAR_REG) && !this.modelType.equals(MODEL_TYPE_LOGISTIC_REG)) {
            throw new IllegalArgumentException(String.format("ModelType must be `linear_reg` or `logistic_reg` but actual[%s]", modelType));
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Struct struct = c.element();
        BQMLModel estimator = c.sideInput(this.estimatorView);
        final Double res;
        if(MODEL_TYPE_LINEAR_REG.equals(modelType)) {
            res = logit(estimator, struct);
        } else if(MODEL_TYPE_LOGISTIC_REG.equals(modelType)) {
            res = probability(estimator, struct);
        } else {
            throw new IllegalArgumentException(String.format("ModelType must be `linear_reg` or `logistic_reg` but actual[%s]", modelType));
        }
        Struct r = buildResult(struct, res);
        c.output(r);
    }

    private double logit(BQMLModel model, Struct struct) {
        double value = model.getIntercept();
        for(Map.Entry<String, Double> entry : model.getNweights().entrySet()) {
            Double v = entry.getValue() != null ?  entry.getValue() : model.getNvaluesIfNull().getOrDefault(entry.getKey(), 0.0);
            value += getNumericalFieldValue(struct, entry.getKey()) * v;
        }
        for(Map.Entry<String, Map<String, Double>> entry : model.getCweights().entrySet()) {
            String cname = getCategoricalFieldValue(struct, entry.getKey());
            value += entry.getValue().getOrDefault(cname, 0.0);
        }
        return value;
    }

    public double probability(BQMLModel model, Struct struct) {
        double logit = logit(model, struct);
        return 1.0 / (1.0 + Math.exp(-logit));
    }

    public Struct buildResult(Struct struct, double score) {
        Struct.Builder builder = Struct.newBuilder().set(FIELD_NAME_SCORE).to(score);
        if(struct.getColumnType(this.keyFieldName).equals(Type.string())) {
            builder = builder.set(this.keyFieldName).to(struct.getString(this.keyFieldName));
        } else if(struct.getColumnType(this.keyFieldName).equals(Type.int64())) {
            builder = builder.set(this.keyFieldName).to(struct.getLong(this.keyFieldName));
        } else {
            throw new IllegalArgumentException(String.format("keyField type must be STRING or INT64! (%s)", struct.getColumnType(this.keyFieldName).toString()));
        }
        return builder.build();
    }

    private static Double getNumericalFieldValue(Struct struct, String columnName) {
        if(struct.isNull(columnName)) {
            return 0.0;
        }
        switch(struct.getColumnType(columnName).getCode()) {
            case FLOAT64:
                return struct.getDouble(columnName);
            case INT64:
                return (double)struct.getLong(columnName);
            case BOOL:
                return struct.getBoolean(columnName) ? 1.0 : 0.0;
            default:
                return 0.0;
        }
    }

    private static String getCategoricalFieldValue(Struct struct, String columnName) {
        if(struct.isNull(columnName)) {
            return "";
        }
        switch(struct.getColumnType(columnName).getCode()) {
            case STRING:
                return struct.getString(columnName);
            case INT64:
                return Long.toString(struct.getLong(columnName));
            case FLOAT64:
                return Double.toString(struct.getDouble(columnName));
            case BOOL:
                return struct.getBoolean(columnName) ? "true" : "false";
            case DATE:
                return struct.getDate(columnName).toString();
            case BYTES:
                return struct.getBytes(columnName).toBase64();
            default:
                return "";
        }
    }
}
