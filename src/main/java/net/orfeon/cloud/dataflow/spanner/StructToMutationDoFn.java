package net.orfeon.cloud.dataflow.spanner;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;

public class StructToMutationDoFn extends DoFn<Struct, Mutation> {

    private final ValueProvider<String> table;
    private String tableString;

    public StructToMutationDoFn(ValueProvider<String> table) {
        this.table = table;
    }

    @Setup
    public void setup() {
        this.tableString = this.table.get();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Struct struct = c.element();
        Mutation mutation = StructUtil.toMutation(struct, this.tableString);
        c.output(mutation);
    }
}
