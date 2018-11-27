package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.ValueBinder;

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
                    builder = binder.to(resultSet.getBigDecimal(column).doubleValue());
                    break;
                case Types.DECIMAL:
                    builder = binder.to(resultSet.getBigDecimal(column).doubleValue());
                    break;
                case Types.INTEGER:
                    builder = binder.to(resultSet.getInt(column));
                    break;
                case Types.SMALLINT:
                    builder = binder.to(resultSet.getShort(column));
                    break;
                case Types.FLOAT:
                    builder = binder.to(resultSet.getFloat(column));
                    break;
                case Types.REAL:
                    builder = binder.to(resultSet.getFloat(column));
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
                    final LocalDate localDate = resultSet.getDate(column).toLocalDate();
                    final Date date = Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
                    builder = binder.to(date);
                    break;
                case Types.TIME:
                    final Time time = resultSet.getTime(column);
                    builder = binder.to(time.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
                    break;
                case Types.TIMESTAMP:
                    builder = binder.to(Timestamp.of(resultSet.getTimestamp(column)));
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
                    builder = binder.to(timeWT.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    builder = binder.to(Timestamp.of(resultSet.getTimestamp(column)));
                    break;
                case Types.LONGVARCHAR:
                    builder = binder.to(resultSet.getString(column));
                    break;
                case Types.BINARY:
                    builder = binder.to(ByteArray.copyFrom(resultSet.getBytes(column)));
                    break;
                case Types.VARBINARY:
                    builder = binder.to(ByteArray.copyFrom(resultSet.getBytes(column)));
                    break;
                case Types.LONGVARBINARY:
                    builder = binder.to(ByteArray.copyFrom(resultSet.getBytes(column)));
                    break;
                case Types.BIGINT:
                    builder = binder.to(resultSet.getLong(column));
                    break;
                case Types.TINYINT:
                    builder = binder.to(resultSet.getByte(column));
                    break;
                case Types.BIT:
                    builder = binder.to(resultSet.getBoolean(column));
                    break;
                case Types.ROWID:
                    builder = binder.to(resultSet.getRowId(column).toString());
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
