package net.orfeon.cloud.dataflow.templates.ml.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import net.orfeon.cloud.dataflow.util.mlmodel.BQMLModel;
import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BQMLLoadCombineFn extends Combine.CombineFn<TableRow, List<TableRow>, BQMLModel> {

    private static final String COLUMN_NAME = "processed_input";
    private static final String COLUMN_WEIGHT = "weight";
    private static final String COLUMN_INTERCEPT = "__INTERCEPT__";
    private static final String COLUMN_CATEGORY_WEIGHT = "category_weights";
    private static final String COLUMN_CATEGORY_NAME = "category";
    private static final String COLUMN_FEATURE_AVG = "mean";

    private static final Logger LOG = LoggerFactory.getLogger(BQMLLoadCombineFn.class);

    @Override
    public List<TableRow> createAccumulator() { return new ArrayList<>(); }

    @Override
    public List<TableRow> addInput(List<TableRow> accum, TableRow value) {
        if(value == null) {
            return accum;
        }
        accum.add(value);
        return accum;
    }

    @Override
    public List<TableRow> mergeAccumulators(Iterable<List<TableRow>> accums) {
        List<TableRow> merged = createAccumulator();
        for(List<TableRow> accum : accums) {
            merged.addAll(accum);
        }
        return merged;
    }

    @Override
    public BQMLModel extractOutput(List<TableRow> accum) {
        Double intercept = 0.0;
        Map<String,Double> nweights = new HashMap<>();
        Map<String,Double> nvaluesIfNull = new HashMap<>();
        Map<String,Map<String,Double>> cweights = new HashMap<>();
        for(TableRow row : accum) {
            String column = (String)row.get(COLUMN_NAME);
            if(COLUMN_INTERCEPT.equals(column)) {
                intercept = (Double)row.get(COLUMN_WEIGHT);
                LOG.info(String.format("intercept: %e", intercept));
                continue;
            }
            Double weight = row.containsKey(COLUMN_WEIGHT) ? (Double)row.get(COLUMN_WEIGHT) : null;
            if(weight != null) {
                nweights.put(column, weight);
                nvaluesIfNull.put(column, (Double)row.get(COLUMN_FEATURE_AVG));
                LOG.info(String.format(String.format("weight[%s]: %e", column, weight)));
            } else {
                cweights.putIfAbsent(column, new HashMap<>());
                List<Map<String,Object>> categoryWeights = (List<Map<String,Object>>)row.get(COLUMN_CATEGORY_WEIGHT);
                categoryWeights.forEach(tr -> {
                    cweights.get(column)
                            .put((String)tr.get(COLUMN_CATEGORY_NAME), (Double)tr.get(COLUMN_WEIGHT));
                    LOG.info(String.format(String.format("category[%s] weight[%s]: %e", column, tr.get(COLUMN_CATEGORY_NAME), tr.get(COLUMN_WEIGHT))));
                });
            }
        }
        BQMLModel model = new BQMLModel();
        model.setNweights(nweights);
        model.setCweights(cweights);
        model.setNvaluesIfNull(nvaluesIfNull);
        model.setIntercept(intercept);
        return model;
    }

}