package net.orfeon.cloud.dataflow.util.mlmodel;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Map;

@DefaultCoder(AvroCoder.class)
public class BQMLModel implements Serializable {

    @Nullable
    private Map<String,Double> nweights;
    @Nullable
    private Map<String,Map<String,Double>> cweights;
    @Nullable
    private Double intercept;
    @Nullable
    private Map<String,Double> nvaluesIfNull;

    public Map<String, Double> getNweights() {
        return nweights;
    }

    public void setNweights(Map<String, Double> nweights) {
        this.nweights = nweights;
    }

    public Map<String, Map<String, Double>> getCweights() {
        return cweights;
    }

    public void setCweights(Map<String, Map<String, Double>> cweights) {
        this.cweights = cweights;
    }

    public Double getIntercept() {
        return intercept;
    }

    public void setIntercept(Double intercept) {
        this.intercept = intercept;
    }

    public Map<String, Double> getNvaluesIfNull() {
        return nvaluesIfNull;
    }

    public void setNvaluesIfNull(Map<String, Double> nvaluesIfNull) {
        this.nvaluesIfNull = nvaluesIfNull;
    }
}
