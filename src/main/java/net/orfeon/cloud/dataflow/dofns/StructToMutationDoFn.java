package net.orfeon.cloud.dataflow.dofns;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.util.converter.StructToMutationConverter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructToMutationDoFn extends DoFn<Struct, Mutation> {

    private static final Logger LOG = LoggerFactory.getLogger(StructToMutationDoFn.class);

    private final ValueProvider<String> tableVP;
    private final ValueProvider<String> mutationOpVP;
    private String table;
    private Mutation.Op mutationOp;

    public StructToMutationDoFn(ValueProvider<String> tableVP, ValueProvider<String> mutationOpVP) {
        this.tableVP = tableVP;
        this.mutationOpVP = mutationOpVP;
    }

    @Setup
    public void setup() {
        this.table = this.tableVP.get();
        this.mutationOp = Mutation.Op.valueOf(this.mutationOpVP.get());
        LOG.info(String.format("StructToMutationDoFn setup finished. table:[%s], op:[%s]", this.table, this.mutationOp));
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Struct struct = c.element();
        Mutation mutation = StructToMutationConverter.convert(struct, this.table, this.mutationOp);
        c.output(mutation);
    }

}
