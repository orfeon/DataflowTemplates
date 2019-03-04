package net.orfeon.cloud.dataflow.transforms;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.Partition;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import java.util.*;


public class SpannerQueryIO {

    private static final String SQL_SPLITTER = "--SPLITTER--";

    public static Read read(ValueProvider<String> projectId,
                            ValueProvider<String> instanceId,
                            ValueProvider<String> databaseId,
                            ValueProvider<String> query,
                            ValueProvider<String> timestampBound) {

        return new Read(projectId, instanceId, databaseId, query, timestampBound);
    }


    public static class Read extends PTransform<PBegin, PCollection<Struct>> {

        public final TupleTag<KV<String, KV<BatchTransactionId, Partition>>> tagOutputPartition
                = new TupleTag<KV<String, KV<BatchTransactionId, Partition>>>(){ private static final long serialVersionUID = 1L; };
        public final TupleTag<Struct> tagOutputStruct
                = new TupleTag<Struct>(){ private static final long serialVersionUID = 1L; };

        private final ValueProvider<String> projectId;
        private final ValueProvider<String> instanceId;
        private final ValueProvider<String> databaseId;
        private final ValueProvider<String> query;
        private final ValueProvider<String> timestampBound;

        private Read(ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> databaseId,
                                ValueProvider<String> query, ValueProvider<String> timestampBound) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.query = query;
            this.timestampBound = timestampBound;
        }

        public PCollection<Struct> expand(PBegin begin) {
            final PCollection<String> queries = begin.getPipeline()
                    .apply("SupplyQuery", Create.ofProvider(this.query, StringUtf8Coder.of()))
                    .apply("SplitQuery", FlatMapElements.into(TypeDescriptors.strings()).via(s -> Arrays.asList(s.split(SQL_SPLITTER))));

            final PCollectionTuple results = queries
                    .apply("ExecuteQuery", ParDo.of(new QueryPartitionSpannerDoFn(this.projectId, this.instanceId, this.databaseId, this.timestampBound)).withOutputTags(tagOutputPartition, TupleTagList.of(tagOutputStruct)));

            final PCollection<Struct> struct1 = results.get(tagOutputPartition)
                    .apply("GroupByPartition", GroupByKey.create())
                    .apply("ReadStruct", ParDo.of(new ReadStructSpannerDoFn(this.projectId, this.instanceId, this.databaseId)));

            final PCollection<Struct> struct2 = results.get(tagOutputStruct);

            return PCollectionList.of(struct1).and(struct2)
                    .apply("Flatten", Flatten.pCollections());
        }

        public class QueryPartitionSpannerDoFn extends DoFn<String, KV<String, KV<BatchTransactionId, Partition>>> {

            private final Logger log = LoggerFactory.getLogger(QueryPartitionSpannerDoFn.class);

            private final ValueProvider<String> projectId;
            private final ValueProvider<String> instanceId;
            private final ValueProvider<String> databaseId;
            private final ValueProvider<String> timestampBound;

            private Spanner spanner;
            private BatchClient batchClient;

            private QueryPartitionSpannerDoFn(ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> databaseId, ValueProvider<String> timestampBound) {
                this.projectId = projectId;
                this.instanceId = instanceId;
                this.databaseId = databaseId;
                this.timestampBound = timestampBound;
            }

            @Setup
            public void setup() {
                // TODO: ENABLE TO SET TIMEOUT. CURRENT DEFAULT TIMEOUT 1 HOUR.
                // In current client status, we can not specify timeout configuration.
                // https://github.com/googleapis/google-cloud-java/issues/3616
                this.spanner = SpannerOptions.newBuilder()
                        .setRetrySettings(RetrySettings.newBuilder().setTotalTimeout(Duration.ofHours(4)).build())
                        .build()
                        .getService();
                this.batchClient = spanner.getBatchClient(
                        DatabaseId.of(projectId.get(), instanceId.get(), databaseId.get()));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String query = c.element();
                final String timestampBoundString = this.timestampBound.get();
                log.info(String.format("Received query [%s], timestamp bound [%s]", query, timestampBoundString));
                final Statement statement = Statement.of(query);

                final TimestampBound tb;
                if(timestampBoundString == null) {
                    tb = TimestampBound.strong();
                } else {
                    final Instant instant = Instant.parse(timestampBoundString);
                    final com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.ofTimeMicroseconds(instant.getMillis() * 1000);
                    tb = TimestampBound.ofReadTimestamp(timestamp);
                }

                try {
                    final BatchReadOnlyTransaction transaction = this.batchClient.batchReadOnlyTransaction(tb); // DO NOT CLOSE!!!
                    final PartitionOptions options = PartitionOptions.newBuilder()
                            //.setMaxPartitions(10000) // Note: this hint is currently ignored in v1.
                            //.setPartitionSizeBytes(100000000) // Note: this hint is currently ignored in v1.
                            .build();
                    final List<Partition> partitions = transaction.partitionQuery(options, statement);
                    log.info(String.format("Query [%s] (with timestamp bound [%s]) divided to [%d] partitions.", query, tb, partitions.size()));
                    for (int i = 0; i < partitions.size(); ++i) {
                        final KV<BatchTransactionId, Partition> value = KV.of(transaction.getBatchTransactionId(), partitions.get(i));
                        final String key = String.format("%d-%s", i, query);
                        final KV<String, KV<BatchTransactionId, Partition>> kv = KV.of(key, value);
                        c.output(kv);
                    }
                } catch (SpannerException e) {
                    if(!e.getErrorCode().equals(ErrorCode.INVALID_ARGUMENT)) {
                        throw e;
                    }
                    log.warn(String.format("Query [%s] could not be executed. Retrying as single query.", query));
                    final DatabaseClient client = spanner.getDatabaseClient(
                            DatabaseId.of(projectId.get(), instanceId.get(), databaseId.get()));

                    try(final ReadOnlyTransaction singleUseTransaction = client.singleUseReadOnlyTransaction(tb);
                        final ResultSet resultSet = singleUseTransaction.executeQuery(statement)) {

                        log.info(String.format("Query [%s] (with timestamp bound [%s]).", query, tb));
                        int count = 0;
                        while(resultSet.next()) {
                            c.output(tagOutputStruct, resultSet.getCurrentRowAsStruct());
                            count++;
                        }
                        log.info(String.format("Query read record num [%d]", count));
                    }
                }
            }

            @Teardown
            public void teardown() {
                this.spanner.close();
            }

        }

        public class ReadStructSpannerDoFn extends DoFn<KV<String, Iterable<KV<BatchTransactionId, Partition>>>, Struct> {

            private final Logger log = LoggerFactory.getLogger(ReadStructSpannerDoFn.class);

            private final ValueProvider<String> projectId;
            private final ValueProvider<String> instanceId;
            private final ValueProvider<String> databaseId;

            private Spanner spanner;
            private BatchClient batchClient;

            private ReadStructSpannerDoFn(ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> databaseId) {
                this.projectId = projectId;
                this.instanceId = instanceId;
                this.databaseId = databaseId;
            }

            @Setup
            public void setup() {
                // TODO: ENABLE TO SET TIMEOUT. CURRENT DEFAULT TIMEOUT 1 HOUR.
                // In current client status, we can not specify timeout configuration.
                // https://github.com/googleapis/google-cloud-java/issues/3616
                // https://stackoverflow.com/questions/44312793/way-to-prevent-transaction-timeout
                this.spanner = SpannerOptions.newBuilder()
                        .setRetrySettings(RetrySettings.newBuilder().setTotalTimeout(Duration.ofHours(4)).build())
                        .build()
                        .getService();
                this.batchClient = spanner.getBatchClient(
                        DatabaseId.of(projectId.get(), instanceId.get(), databaseId.get()));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final KV<String, Iterable<KV<BatchTransactionId, Partition>>> kv = c.element();
                final String partitionNumberQuery = kv.getKey();
                final KV<BatchTransactionId, Partition> value = kv.getValue().iterator().next();
                final BatchTransactionId transactionId = value.getKey();
                final BatchReadOnlyTransaction transaction = this.batchClient.batchReadOnlyTransaction(transactionId); // DO NOT CLOSE!!!
                final Partition partition = value.getValue();

                try(final ResultSet resultSet = transaction.execute(partition)) {
                    log.info(String.format("Started %s th partition[%s] query.", partitionNumberQuery.split("-")[0], partition));
                    int count = 0;
                    while (resultSet.next()) {
                        c.output(resultSet.getCurrentRowAsStruct());
                        count++;
                    }
                    log.info(String.format("%s th partition completed to read record: [%d]", partitionNumberQuery.split("-")[0], count));
                }
            }

            @Teardown
            public void teardown() {
                this.spanner.close();
            }

        }

    }

}
