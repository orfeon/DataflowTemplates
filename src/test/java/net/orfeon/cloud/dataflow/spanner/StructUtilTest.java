package net.orfeon.cloud.dataflow.spanner;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class StructUtilTest {

    @Test
    public void test() {

        Date date1 = Date.fromYearMonthDay(2018, 9, 1);
        Date date2 = Date.fromYearMonthDay(2018, 10, 1);
        String str1 = "2018-09-01T12:00+09:00";
        String str2 = "2018-10-01T12:00+09:00";
        Instant instant1 = Instant.parse(str1);
        Instant instant2 = Instant.parse(str2);
        Timestamp timestamp1 = Timestamp.ofTimeMicroseconds(instant1.getMillis() * 1000);
        Timestamp timestamp2 = Timestamp.ofTimeMicroseconds(instant2.getMillis() * 1000);

        Struct struct = Struct.newBuilder()
                .set("bf").to(false)
                .set("if").to(-12)
                .set("ff").to(110.005)
                .set("sf").to("I am a pen")
                .set("df").to(date2)
                .set("tf").to(timestamp2)
                .set("nsf").to((String)null)
                .set("nff").to((Double)null)
                .set("ndf").to((Date)null)
                .set("ntf").to((Timestamp)null)
                .set("asf").toStringArray(Arrays.asList("a", "b", "c"))
                .set("aif").toInt64Array(Arrays.asList(1L, 2L, 3L))
                .set("adf").toDateArray(Arrays.asList(date1, date2))
                .set("atf").toTimestampArray(Arrays.asList(timestamp1, timestamp2))
                .build();

        Mutation mutation = StructUtil.toMutation(struct, "mytable");
        Assert.assertEquals("mytable", mutation.getTable());
        System.out.println(mutation);
        Map<String, Value> map = mutation.asMap();
        Assert.assertEquals(struct.getString("sf"), map.get("sf").getString());
        Assert.assertEquals(struct.getBoolean("bf"), map.get("bf").getBool());
        Assert.assertEquals(struct.getLong("if"), map.get("if").getInt64());
        Assert.assertEquals(struct.getDouble("ff"), map.get("ff").getFloat64(), 0);
        Assert.assertEquals(struct.getDate("df"), map.get("df").getDate());
        Assert.assertEquals(struct.getTimestamp("tf"), map.get("tf").getTimestamp());
        Assert.assertEquals(struct.getStringList("asf"), map.get("asf").getStringArray());
        Assert.assertEquals(struct.getLongList("aif"), map.get("aif").getInt64Array());
        Assert.assertEquals(struct.getDateList("adf"), map.get("adf").getDateArray());
        Assert.assertEquals(struct.getTimestampList("atf"), map.get("atf").getTimestampArray());
        Assert.assertTrue(map.get("nsf").isNull());
        Assert.assertTrue(map.get("nff").isNull());
        Assert.assertTrue(map.get("ndf").isNull());
        Assert.assertTrue(map.get("ntf").isNull());


    }
}
