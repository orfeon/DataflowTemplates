package net.orfeon.cloud.dataflow.spanner;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.transforms.DoFn;


public class StructToTableRowDoFn extends DoFn<Struct, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        final Struct struct = c.element();
        final TableRow row = StructUtil.toTableRow(struct);
        c.output(row);
    }

}
