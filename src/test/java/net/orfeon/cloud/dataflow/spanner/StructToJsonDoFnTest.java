package net.orfeon.cloud.dataflow.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class StructToJsonDoFnTest {

    @Before
    public void setUp() {
        System.out.println("setup");
    }

    @After
    public void tearDown() {
        System.out.println("teardown");
    }

    @Test
    public void test() {
        Struct struct1 = Struct.newBuilder()
                .add("bool", Value.bool(true))
                .add("int", Value.int64(12))
                .add("string", Value.string("string"))
                .add("float", Value.float64(10.12))
                .add("timestamp", Value.timestamp(Timestamp.parseTimestamp("2018-01-19T03:24:13Z")))
                .build();
        Struct struct2 = Struct.newBuilder()
                .add("bool", Value.bool(false))
                .add("int", Value.int64(-10))
                .add("string", Value.string("this is a pen!"))
                .add("float", Value.float64(0.12))
                .add("timestamp", Value.timestamp(Timestamp.parseTimestamp("2018-10-01T12:00:00Z")))
                .build();


        String str = "2018-01-19 03:24:13 UTC";//"2018-01-19T03:24:13Z";
        str = str.replace(" UTC", "Z").replace(" ","T");
        com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.parseTimestamp(str);

        Pipeline pipeline = Pipeline.create();
        PCollection<String> lines = pipeline
                .apply("CreateDummy", Create.of(struct1, struct2))
                .apply("ConvertToJson", ParDo.of(new StructToJsonDoFn()));

        PAssert.that(lines).containsInAnyOrder(
                "{\"bool\":true,\"int\":12,\"string\":\"string\",\"float\":10.12,\"timestamp\":1516332253}",
                "{\"bool\":false,\"int\":-10,\"string\":\"this is a pen!\",\"float\":0.12,\"timestamp\":1538395200}");

        pipeline.run();
    }

}
