package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.spanner.Struct;
import net.orfeon.cloud.dataflow.spanner.StructToAvroTransform;
import net.orfeon.cloud.dataflow.spanner.StructUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;


public class JdbcToAvro {

    private static final String JDBC_URL_TMPL = "jdbc:mysql://google/%s?cloudSqlInstance=%s&socketFactory=com.google.cloud.sql.mysql.SocketFactory";

    public interface JdbcToAvroPipelineOption extends PipelineOptions {

        @Description("SQL query to extract records")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("GCS path to output. prefix must start with gs://")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> output);

        @Description("DriverClass, `com.mysql.cj.jdbc.Driver` or `org.postgresql.Driver`")
        ValueProvider<String> getDriverClass();
        void setDriverClass(ValueProvider<String> fieldKey);

        @Description("Connection endpoint, format: {projectID}:{zone}:{instanceID}.{database}")
        ValueProvider<String> getConnectionString();
        void setConnectionString(ValueProvider<String> connectionString);

        @Description("Database user to access")
        ValueProvider<String> getUsername();
        void setUsername(ValueProvider<String> username);

        @Description("Database access user's password")
        ValueProvider<String> getPassword();
        void setPassword(ValueProvider<String> password);

        @Description("Struct field key to separate output path")
        ValueProvider<String> getFieldKey();
        void setFieldKey(ValueProvider<String> fieldKey);

        @Description("Use snappy or default codec")
        @Default.Boolean(true)
        ValueProvider<Boolean> getUseSnappy();
        void setUseSnappy(ValueProvider<Boolean> useSnappy);

    }

    public static void main(String[] args) {

        JdbcToAvroPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(JdbcToAvroPipelineOption.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Query", JdbcIO.<Struct>read()
                    .withQuery(options.getQuery())
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                            .create(options.getDriverClass(), ValueProvider.NestedValueProvider.of(
                                    options.getConnectionString(),
                                    s -> String.format(JDBC_URL_TMPL, s.split("\\.")[1], s.split("\\.")[0])))
                            .withUsername(options.getUsername())
                            .withPassword(options.getPassword()))
                    .withRowMapper(resultSet -> StructUtil.from(resultSet))
                    .withCoder(SerializableCoder.of(Struct.class)))
                .apply("StoreGCSAvro", new StructToAvroTransform(options.getOutput(), options.getFieldKey(), options.getUseSnappy()));

        pipeline.run();
    }
}
