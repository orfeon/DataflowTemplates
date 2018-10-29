package net.orfeon.cloud.dataflow.spanner;

import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class StructToCsvDoFnTest {

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
                .set("bool").to(true)
                .set("int").to(12)
                .set("string").to("string")
                .set("float").to(10.12)
                //.add("", Value.timestamp(Timestamp.from()))
                .build();
        Struct struct2 = Struct.newBuilder()
                .set("bool").to(false)
                .set("int").to(-10)
                .set("string").to("this is a pen!")
                .set("float").to(0.12)
                //.add("", Value.timestamp(Timestamp.from()))
                .build();

        Pipeline pipeline = Pipeline.create();
        PCollection<String> lines = pipeline
                .apply("CreateDummy", Create.of(struct1, struct2))
                .apply("ConvertToCsv", ParDo.of(new StructToCsvDoFn()));

        PAssert.that(lines).containsInAnyOrder(
                "true,12,string,10.12",
                "false,-10,this is a pen!,0.12");

        pipeline.run();
    }

}
