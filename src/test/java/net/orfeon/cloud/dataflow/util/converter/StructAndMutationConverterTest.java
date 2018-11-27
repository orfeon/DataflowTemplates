package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class StructAndMutationConverterTest {

    @Test
    public void testMutationConvert() {
        // Test struct to mutation
        Struct struct1 = ConverterDataSupplier.createSimpleStruct();
        Mutation mutation = StructToMutationConverter.convert(struct1, "mytable", Mutation.Op.valueOf("INSERT"));
        Assert.assertEquals("mytable", mutation.getTable());
        System.out.println(mutation);
        Map<String, Value> map = mutation.asMap();
        Assert.assertEquals(struct1.getString("sf"), map.get("sf").getString());
        Assert.assertEquals(struct1.getBoolean("bf"), map.get("bf").getBool());
        Assert.assertEquals(struct1.getLong("if"), map.get("if").getInt64());
        Assert.assertEquals(struct1.getDouble("ff"), map.get("ff").getFloat64(), 0);
        Assert.assertEquals(struct1.getDate("df"), map.get("df").getDate());
        Assert.assertEquals(struct1.getTimestamp("tf"), map.get("tf").getTimestamp());
        Assert.assertEquals(struct1.getStringList("asf"), map.get("asf").getStringArray());
        Assert.assertEquals(struct1.getLongList("aif"), map.get("aif").getInt64Array());
        Assert.assertEquals(struct1.getDateList("adf"), map.get("adf").getDateArray());
        Assert.assertEquals(struct1.getTimestampList("atf"), map.get("atf").getTimestampArray());
        Assert.assertTrue(map.get("nsf").isNull());
        Assert.assertTrue(map.get("nff").isNull());
        Assert.assertTrue(map.get("ndf").isNull());
        Assert.assertTrue(map.get("ntf").isNull());

        // Test mutation to struct
        Struct struct2 = MutationToStructConverter.convert(mutation);
        System.out.println(struct2);
        Assert.assertEquals(struct2.getString("sf"), map.get("sf").getString());
        Assert.assertEquals(struct2.getBoolean("bf"), map.get("bf").getBool());
        Assert.assertEquals(struct2.getLong("if"), map.get("if").getInt64());
        Assert.assertEquals(struct2.getDouble("ff"), map.get("ff").getFloat64(), 0);
        Assert.assertEquals(struct2.getDate("df"), map.get("df").getDate());
        Assert.assertEquals(struct2.getTimestamp("tf"), map.get("tf").getTimestamp());
        Assert.assertEquals(struct2.getStringList("asf"), map.get("asf").getStringArray());
        Assert.assertEquals(struct2.getLongList("aif"), map.get("aif").getInt64Array());
        Assert.assertEquals(struct2.getDateList("adf"), map.get("adf").getDateArray());
        Assert.assertEquals(struct2.getTimestampList("atf"), map.get("atf").getTimestampArray());
    }

    @Test
    public void testMutationGroupConvert() {
        Struct struct1 = ConverterDataSupplier.createSimpleStruct();
        Struct struct2 = ConverterDataSupplier.createNestedStruct(false);
        Mutation mutation1 = StructToMutationConverter.convert(struct1, "mytable1", Mutation.Op.valueOf("INSERT"));
        Mutation mutation2 = StructToMutationConverter.convert(struct2, "mytable2", Mutation.Op.valueOf("INSERT"));
        MutationGroup mutationGroup = MutationGroup.create(mutation1, mutation2);
        List<Struct> structList = MutationToStructConverter.convert(mutationGroup);
        System.out.println(structList);
    }
}
