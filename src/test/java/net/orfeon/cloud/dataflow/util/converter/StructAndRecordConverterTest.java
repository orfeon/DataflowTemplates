package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StructAndRecordConverterTest {

    private static final MutableDateTime EPOCHDATETIME = new MutableDateTime(0, DateTimeZone.UTC);

    @Test
    public void testSchema() {
        Struct struct1 = ConverterDataSupplier.createSimpleStruct();
        Schema schema = StructToRecordConverter.convertSchema(struct1);
        System.out.println(schema);
    }

    @Test
    public void test() {
        Struct struct1 = ConverterDataSupplier.createSimpleStruct();
        Struct struct2 = ConverterDataSupplier.createNestedStruct(true);
        Schema schema = StructToRecordConverter.convertSchema(struct2);
        System.out.println(schema);
        GenericRecord r = StructToRecordConverter.convert(struct2, schema);
        System.out.println(r);

        Date date1 = struct1.getDate("cdf");
        Date date2 = struct2.getDate("df");
        Timestamp timestamp1 = struct1.getTimestamp("ctf");
        Timestamp timestamp2 = struct2.getTimestamp("tf");

        Assert.assertFalse((Boolean)r.get("bf"));
        Assert.assertEquals(-12L, (long)r.get("if"));
        Assert.assertEquals(110.005, (double)r.get("ff"), 0);
        Assert.assertEquals("I am a pen", r.get("sf"));
        Assert.assertEquals(null, r.get("nf"));
        Assert.assertEquals(getEpochDays(date2), r.get("df"));
        Assert.assertEquals(timestamp2.getSeconds()*1000, r.get("tf"));

        Assert.assertArrayEquals(new Long[]{1L, 2L, 3L}, ((List<Long>)(r.get("aif"))).toArray());
        Assert.assertNull(r.get("lnf"));
        Assert.assertNull(r.get("dnf"));
        Assert.assertNull(r.get("tnf"));
        Assert.assertNull(r.get("anf"));
        Assert.assertArrayEquals(new Long[]{null, 2L, 3L}, ((List<Long>)(r.get("amf"))).toArray());
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

        Struct struct3 = RecordToStructConverter.convert(r);
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
        Assert.assertArrayEquals(struct2.getLongList("amf").toArray(), struct3.getLongList("amf").toArray());
        Assert.assertArrayEquals(struct2.getTimestampList("atf").toArray(), struct3.getTimestampList("atf").toArray());
    }

    private int getEpochDays(Date date) {
        DateTime datetime = new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
        Days days = Days.daysBetween(EPOCHDATETIME, datetime);
        return days.getDays();
    }
}
