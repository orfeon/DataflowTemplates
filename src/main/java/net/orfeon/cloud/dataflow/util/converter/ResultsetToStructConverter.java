package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.ValueBinder;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


public class ResultsetToStructConverter {

    private ResultsetToStructConverter() {

    }

    public static Struct convert(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int columnCount = meta.getColumnCount();
        Struct.Builder builder = Struct.newBuilder();
        for (int column = 1; column <= columnCount; ++column) {
            final ValueBinder<Struct.Builder> binder = builder.set(meta.getColumnName(column));
            switch (meta.getColumnType(column)) {
                case Types.CHAR:
                    builder = binder.to(resultSet.getString(column));
                    break;
                case Types.NUMERIC:
                    BigDecimal bigDecimal = resultSet.getBigDecimal(column);
                    if(bigDecimal == null) {
                        builder = binder.to((Double) null);
                        break;
                    }
                    builder = binder.to(bigDecimal.doubleValue());
                    break;
                case Types.DECIMAL:
                    BigDecimal decimal = resultSet.getBigDecimal(column);
                    if(decimal == null) {
                        builder = binder.to((Double) null);
                        break;
                    }
                    builder = binder.to(decimal.doubleValue());
                    break;
                case Types.INTEGER:
                    builder = binder.to(resultSet.getInt(column));
                    break;
                case Types.SMALLINT:
                    builder = binder.to(resultSet.getInt(column));
                    break;
                case Types.FLOAT:
                    builder = binder.to(resultSet.getDouble(column));
                    break;
                case Types.REAL:
                    builder = binder.to(resultSet.getDouble(column));
                    break;
                case Types.DOUBLE:
                    builder = binder.to(resultSet.getDouble(column));
                    break;
                case Types.VARCHAR:
                    builder = binder.to(resultSet.getString(column));
                    break;
                case Types.BOOLEAN:
                    builder = binder.to(resultSet.getBoolean(column));
                    break;
                case Types.DATALINK:
                    break;
                case Types.DATE:
                    final java.sql.Date sqlDate = resultSet.getDate(column);
                    if(sqlDate == null) {
                        builder = binder.to((Date) null);
                        break;
                    }
                    final LocalDate localDate = sqlDate.toLocalDate();
                    final Date date = Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
                    builder = binder.to(date);
                    break;
                case Types.TIME:
                    final Time time = resultSet.getTime(column);
                    if(time == null) {
                        builder = binder.to((String) null);
                        break;
                    }
                    builder = binder.to(time.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
                    break;
                case Types.TIMESTAMP:
                    final java.sql.Timestamp timestamp = resultSet.getTimestamp(column);
                    if(timestamp == null) {
                        builder = binder.to((Timestamp) null);
                        break;
                    }
                    builder = binder.to(Timestamp.of(timestamp));
                    break;
                case Types.JAVA_OBJECT:
                    break;
                case Types.DISTINCT:
                    break;
                case Types.STRUCT:
                    break;
                case Types.ARRAY:
                    break;
                case Types.BLOB:
                    break;
                case Types.CLOB:
                    break;
                case Types.REF:
                    break;
                case Types.SQLXML:
                    break;
                case Types.NCLOB:
                    break;
                case Types.REF_CURSOR:
                    break;
                case Types.TIME_WITH_TIMEZONE:
                    final Time timeWT = resultSet.getTime(column);
                    if(timeWT == null) {
                        builder = binder.to((String) null);
                        break;
                    }
                    builder = binder.to(timeWT.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    final java.sql.Timestamp timestampWT = resultSet.getTimestamp(column);
                    if(timestampWT == null) {
                        builder = binder.to((Timestamp) null);
                        break;
                    }
                    builder = binder.to(Timestamp.of(timestampWT));
                    break;
                case Types.LONGVARCHAR:
                    builder = binder.to(resultSet.getString(column));
                    break;
                case Types.BINARY:
                    byte[] binary = resultSet.getBytes(column);
                    if(binary == null) {
                        builder = binder.to((ByteArray) null);
                        break;
                    }
                    builder = binder.to(ByteArray.copyFrom(binary));
                    break;
                case Types.VARBINARY:
                    byte[] varbinary = resultSet.getBytes(column);
                    if(varbinary == null) {
                        builder = binder.to((ByteArray) null);
                        break;
                    }
                    builder = binder.to(ByteArray.copyFrom(varbinary));
                    break;
                case Types.LONGVARBINARY:
                    byte[] longvarbinary = resultSet.getBytes(column);
                    if(longvarbinary == null) {
                        builder = binder.to((ByteArray) null);
                        break;
                    }
                    builder = binder.to(ByteArray.copyFrom(longvarbinary));
                    break;
                case Types.BIGINT:
                    builder = binder.to(resultSet.getLong(column));
                    break;
                case Types.TINYINT:
                    builder = binder.to(resultSet.getInt(column));
                    break;
                case Types.BIT:
                    builder = binder.to(resultSet.getBoolean(column));
                    break;
                case Types.ROWID:
                    RowId rowId = resultSet.getRowId(column);
                    if(rowId == null) {
                        builder = binder.to((String) null);
                        break;
                    }
                    builder = binder.to(rowId.toString());
                    break;
                case Types.NVARCHAR:
                    builder = binder.to(resultSet.getNString(column));
                    break;
                case Types.NCHAR:
                    builder = binder.to(resultSet.getNString(column));
                    break;
                case Types.LONGNVARCHAR:
                    builder = binder.to(resultSet.getNString(column));
                    break;
            }
        }
        return builder.build();
    }

}
