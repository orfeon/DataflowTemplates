package net.orfeon.cloud.dataflow.transforms;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.kms.v1.*;
import com.google.cloud.spanner.*;
import com.google.protobuf.ByteString;
import net.orfeon.cloud.dataflow.util.converter.ResultsetToStructConverter;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Base64;


public class JdbcSimpleIO {

    private static final String SQL_SPLITTER = "--SPLITTER--";

    public static Read read(ValueProvider<String> driverClassName, ValueProvider<String> url, ValueProvider<String> username, ValueProvider<String> password, ValueProvider<String> query, ValueProvider<String> cryptoKeyName) {
        return new Read(driverClassName, url, username, password, query, cryptoKeyName);
    }

    public static class Read extends PTransform<PBegin, PCollection<Struct>> {

        private final ValueProvider<String> driverClassName;
        private final ValueProvider<String> url;
        private final ValueProvider<String> username;
        private final ValueProvider<String> password;
        private final ValueProvider<String> query;
        private final ValueProvider<String> cryptoKeyName;

        private Read(ValueProvider<String> driverClassName, ValueProvider<String> url, ValueProvider<String> username, ValueProvider<String> password, ValueProvider<String> query, ValueProvider<String> cryptoKeyName) {
            this.driverClassName = driverClassName;
            this.url = url;
            this.username = username;
            this.password = password;
            this.query = query;
            this.cryptoKeyName = cryptoKeyName;
        }

        public PCollection<Struct> expand(PBegin begin) {
            return begin.getPipeline()
                    .apply("SupplyQuery", Create.ofProvider(this.query, StringUtf8Coder.of()))
                    .apply("SplitQuery", FlatMapElements.into(TypeDescriptors.strings()).via(s -> Arrays.asList(s.split(SQL_SPLITTER))))
                    .apply("ToKeyValue", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.booleans())).via(s -> KV.of(s, true)))
                    .apply("GroupByQuery", GroupByKey.create())
                    .apply("ExecuteQuery", ParDo.of(new QueryExecuteDoFn(this.driverClassName, this.url, this.username, this.password, this.cryptoKeyName)));
        }

        public class QueryExecuteDoFn extends DoFn<KV<String, Iterable<Boolean>>, Struct> {

            private static final int DEFAULT_FETCH_SIZE = 50_000;
            private final Logger log = LoggerFactory.getLogger(QueryExecuteDoFn.class);

            private final ValueProvider<String> driverClassName;
            private final ValueProvider<String> url;
            private final ValueProvider<String> username;
            private final ValueProvider<String> password;
            private final ValueProvider<String> cryptoKeyName;

            private DataSource dataSource;
            private Connection connection;

            private QueryExecuteDoFn(ValueProvider<String> driverClassName, ValueProvider<String> url, ValueProvider<String> username, ValueProvider<String> password, ValueProvider<String> cryptoKeyName) {
                this.driverClassName = driverClassName;
                this.url = url;
                this.username = username;
                this.password = password;
                this.cryptoKeyName = cryptoKeyName;
            }

            @Setup
            public void setup() throws Exception {
                this.dataSource = buildDataSource(this.driverClassName.get(), this.url.get(), this.username.get(), this.password.get(), this.cryptoKeyName.get());
                this.connection = dataSource.getConnection();
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                final String query = c.element().getKey();
                log.info(String.format("Received query [%s]", query));
                try (PreparedStatement statement = connection
                        .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    statement.setFetchSize(DEFAULT_FETCH_SIZE);
                    int count = 0;
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            Struct struct = ResultsetToStructConverter.convert(resultSet);
                            c.output(struct);
                            count++;
                        }
                    }
                    log.info(String.format("Query [%s] read record num [%d]", query, count));
                }
            }

            @Teardown
            public void teardown() throws Exception {
                this.connection.close();
            }

            private DataSource buildDataSource(String driverClassName, String url, String username, String password, String cryptoKeyName) throws Exception {
                final BasicDataSource basicDataSource = new BasicDataSource();
                basicDataSource.setDriverClassName(driverClassName);
                basicDataSource.setUrl(url);
                basicDataSource.setUsername(username);
                if(cryptoKeyName != null) {
                    password = decrypt(cryptoKeyName, password);
                }
                basicDataSource.setPassword(password);

                // Wrapping the datasource as a pooling datasource
                final DataSourceConnectionFactory connectionFactory = new DataSourceConnectionFactory(basicDataSource);
                final PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
                final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
                poolConfig.setMaxTotal(1);
                poolConfig.setMinIdle(0);
                poolConfig.setMinEvictableIdleTimeMillis(10000);
                poolConfig.setSoftMinEvictableIdleTimeMillis(30000);
                final GenericObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory, poolConfig);
                poolableConnectionFactory.setPool(connectionPool);
                poolableConnectionFactory.setDefaultAutoCommit(false);
                poolableConnectionFactory.setDefaultReadOnly(false);
                return new PoolingDataSource(connectionPool);
            }

            private String decrypt(String key, String body) throws IOException {
                final Credentials credentials =
                        KeyManagementServiceSettings.defaultCredentialsProviderBuilder()
                                .build()
                                .getCredentials();
                final KeyManagementServiceSettings settings =
                        KeyManagementServiceSettings.newBuilder()
                                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                                .build();

                final byte[] decoded = Base64.getDecoder().decode(body);
                try(final KeyManagementServiceClient client = KeyManagementServiceClient.create(settings)) {
                    final DecryptResponse res = client.decrypt(CryptoKeyName.parse(key), ByteString.copyFrom(decoded));
                    return res.getPlaintext().toStringUtf8().trim();
                }

            }

        }

    }

}
