package com.hazelcast.query;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * @author mdogan 7/4/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class DateHelperTest {

    @Test
    public void testSqlDate() {
        final long now = System.currentTimeMillis();

        final java.sql.Date date1 = new java.sql.Date(now);
        final java.sql.Date date2 = DateHelper.parseSqlDate(date1.toString());

        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(date1.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(date2.getTime());

        Assert.assertEquals(cal1.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
        Assert.assertEquals(cal1.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
        Assert.assertEquals(cal1.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    public void testUtilDate() {
        final long now = System.currentTimeMillis();

        final Date date1 = new Date(now);
        final Date date2 = DateHelper.parseDate(date1.toString());

        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(date1.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(date2.getTime());

        Assert.assertEquals(cal1.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
        Assert.assertEquals(cal1.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
        Assert.assertEquals(cal1.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(cal1.get(Calendar.HOUR_OF_DAY), cal2.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(cal1.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
        Assert.assertEquals(cal1.get(Calendar.SECOND), cal2.get(Calendar.SECOND));
    }

    @Test
    public void testTimestamp() {
        final long now = System.currentTimeMillis();

        final Timestamp date1 = new Timestamp(now);
        final Timestamp date2 = DateHelper.parseTimeStamp(date1.toString());

        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(date1.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(date2.getTime());

        Assert.assertEquals(cal1.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
        Assert.assertEquals(cal1.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
        Assert.assertEquals(cal1.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(cal1.get(Calendar.HOUR_OF_DAY), cal2.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(cal1.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
        Assert.assertEquals(cal1.get(Calendar.SECOND), cal2.get(Calendar.SECOND));
        Assert.assertEquals(cal1.get(Calendar.MILLISECOND), cal2.get(Calendar.MILLISECOND));
    }

    @Test
    public void testTime() {
        final long now = System.currentTimeMillis();

        final Time time1 = new Time(now);
        final Time time2 = DateHelper.parseSqlTime(time1.toString());

        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(time1.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(time2.getTime());

        Assert.assertEquals(cal1.get(Calendar.HOUR_OF_DAY), cal2.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(cal1.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
        Assert.assertEquals(cal1.get(Calendar.SECOND), cal2.get(Calendar.SECOND));
    }
}
