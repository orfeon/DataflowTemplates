package net.orfeon.cloud.dataflow.storage;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AvroUtilTest {

    private static final MutableDateTime EPOCHDATETIME = new MutableDateTime(0, DateTimeZone.UTC);

    @Test
    public void testSchema() {
        Date date1 = Date.fromYearMonthDay(2018, 9, 1);
        String str1 = "2018-09-01T12:00+09:00";
        Instant instant1 = Instant.parse(str1);
        Timestamp timestamp1 = Timestamp.ofTimeMicroseconds(instant1.getMillis() * 1000);

        Struct struct1 = Struct.newBuilder()
                .set("cbf").to(true)
                .set("cif").to(12)
                .set("cff").to(0.005)
                .set("cdf").to(date1)
                .set("ctf").to(timestamp1)
                .set("csf").to("This is a pen")
                .build();

        Schema schema = AvroUtil.convertSchemaFromStruct(struct1);
        System.out.println(schema);
    }

    @Test
    public void testConvert() {

        Date date1 = Date.fromYearMonthDay(2018, 9, 1);
        Date date2 = Date.fromYearMonthDay(2018, 10, 1);
        String str1 = "2018-09-01T12:00+09:00";
        String str2 = "2018-10-01T12:00+09:00";
        Instant instant1 = Instant.parse(str1);
        Instant instant2 = Instant.parse(str2);
        Timestamp timestamp1 = Timestamp.ofTimeMicroseconds(instant1.getMillis() * 1000);
        Timestamp timestamp2 = Timestamp.ofTimeMicroseconds(instant2.getMillis() * 1000);

        Struct struct1 = Struct.newBuilder()
                .set("cbf").to(true)
                .set("cif").to(12)
                .set("cff").to(0.005)
                .set("cdf").to(date1)
                .set("ctf").to(timestamp1)
                .set("csf").to("This is a pen")
                .build();

        Type.StructField f1 = Type.StructField.of("cbf", Type.bool());
        Type.StructField f2 = Type.StructField.of("cif", Type.int64());
        Type.StructField f3 = Type.StructField.of("cff", Type.float64());
        Type.StructField f4 = Type.StructField.of("cdf", Type.date());
        Type.StructField f5 = Type.StructField.of("ctf", Type.timestamp());
        Type.StructField f6 = Type.StructField.of("csf", Type.string());

        Struct struct2 = Struct.newBuilder()
                .set("bf").to(false)
                .set("if").to(-12)
                .set("ff").to(110.005)
                .set("sf").to("I am a pen")
                .set("df").to(date2)
                .set("tf").to(timestamp2)
                .set("nf").to((String)null)
                .set("rf").to(struct1)
                .set("arf").toStructArray(Type.struct(f1,f2, f3, f4, f5, f6), Arrays.asList(struct1))
                .set("asf").toStringArray(Arrays.asList("a", "b", "c"))
                .set("aif").toInt64Array(Arrays.asList(1L, 2L, 3L))
                .set("adf").toDateArray(Arrays.asList(date1, date2))
                .set("atf").toTimestampArray(Arrays.asList(timestamp1, timestamp2))
                .build();

        Schema schema = AvroUtil.convertSchemaFromStruct(struct2);
        System.out.println(schema);
        GenericRecord r = AvroUtil.convertGenericRecord(struct2, schema);
        System.out.println(r);

        Assert.assertFalse((Boolean)r.get("bf"));
        Assert.assertEquals(-12L, (long)r.get("if"));
        Assert.assertEquals(110.005, (double)r.get("ff"), 0);
        Assert.assertEquals("I am a pen", r.get("sf"));
        Assert.assertEquals(null, r.get("nf"));
        Assert.assertEquals(getEpochDays(date2), r.get("df"));
        Assert.assertEquals(timestamp2.getSeconds()*1000, r.get("tf"));

        Assert.assertArrayEquals(new Long[]{1L, 2L, 3L}, ((List<Long>)(r.get("aif"))).toArray());
        Assert.assertArrayEquals(new String[]{"a", "b", "c"}, ((List<String>)(r.get("asf"))).toArray());
        Assert.assertArrayEquals(new Integer[]{getEpochDays(date1), getEpochDays(date2)}, ((List<String>)(r.get("adf"))).toArray());
        Assert.assertArrayEquals(new Long[]{timestamp1.getSeconds()*1000, timestamp2.getSeconds()*1000}, ((List<Long>)(r.get("atf"))).toArray());

        GenericRecord c = (GenericRecord) r.get("rf");
        Assert.assertTrue((Boolean) c.get("cbf"));
        Assert.assertEquals(12L, c.get("cif"));
        Assert.assertEquals(0.005, c.get("cff"));
        Assert.assertEquals("This is a pen", c.get("csf"));
        Assert.assertEquals(getEpochDays(date1), c.get("cdf"));
        Assert.assertEquals(timestamp1.getSeconds()*1000, c.get("ctf"));

        GenericRecord a = ((List<GenericRecord>)r.get("arf")).get(0);
        Assert.assertTrue((Boolean) a.get("cbf"));
        Assert.assertEquals(12L, a.get("cif"));
        Assert.assertEquals(0.005, a.get("cff"));
        Assert.assertEquals("This is a pen", a.get("csf"));
        Assert.assertEquals(getEpochDays(date1), a.get("cdf"));
        Assert.assertEquals(timestamp1.getSeconds()*1000, a.get("ctf"));

        Struct struct3 = AvroUtil.convertStruct(r);
        System.out.println(struct3);

        Assert.assertEquals(struct2.getBoolean("bf"), struct3.getBoolean("bf"));
        Assert.assertEquals(struct2.getLong("if"), struct3.getLong("if"));
        Assert.assertEquals(struct3.getDouble("ff"), struct3.getDouble("ff"), 0);
        Assert.assertEquals(struct3.getString("sf"), struct3.getString("sf"));
        Assert.assertEquals(struct3.isNull("nf"), struct3.isNull("nf"));
        Assert.assertEquals(struct3.getDate("df"), struct3.getDate("df"));
        Assert.assertEquals(struct3.getTimestamp("tf"), struct3.getTimestamp("tf"));

        Assert.assertArrayEquals(struct2.getLongArray("aif"), struct3.getLongArray("aif"));
        Assert.assertArrayEquals(struct2.getStringList("asf").toArray(), struct3.getStringList("asf").toArray());
        Assert.assertArrayEquals(struct2.getDateList("adf").toArray(), struct3.getDateList("adf").toArray());
        Assert.assertArrayEquals(struct2.getTimestampList("atf").toArray(), struct3.getTimestampList("atf").toArray());
    }

    private int getEpochDays(Date date) {
        DateTime datetime = new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
        Days days = Days.daysBetween(EPOCHDATETIME, datetime);
        return days.getDays();
    }

}
