package net.orfeon.cloud.dataflow.spanner;

import com.google.cloud.spanner.*;
import com.google.common.collect.Iterables;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.cloud.spanner.SpannerOptions.getDefaultInstance;

import java.util.ArrayList;
import java.util.List;

public class SpannerSimpleIO {

    public static Write write(String projectId, String instanceId, String databaseId, int limit) {
        return new Write(projectId, instanceId, databaseId, limit);
    }

    public static Read read(ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> databaseId,
                            ValueProvider<String> query, ValueProvider<String> timestampBound) {
        return new Read(projectId, instanceId, databaseId, query, timestampBound);
    }

    public static ReadSingleQuery readSingle(ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> databaseId,
                            ValueProvider<String> query, ValueProvider<String> timestampBound) {
        return new ReadSingleQuery(projectId, instanceId, databaseId, query, timestampBound);
    }

    public static class Read extends PTransform<PBegin, PCollection<Struct>> {

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
            return  begin.getPipeline()
                    .apply("SupplyQuery", Create.ofProvider(this.query, StringUtf8Coder.of()))
                    .apply("SplitPartition", ParDo.of(new QueryPartitionSpannerDoFn(this.projectId, this.instanceId, this.databaseId, this.timestampBound)))
                    .apply("ReadStruct", ParDo.of(new ReadStructSpannerDoFn(this.projectId, this.instanceId, this.databaseId)));
        }

        public class QueryPartitionSpannerDoFn extends DoFn<String, KV<BatchTransactionId, Partition>> {

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
            public void setup() throws Exception {
                final SpannerOptions options = getDefaultInstance();
                this.spanner = options.getService();
                this.batchClient = spanner.getBatchClient(
                        DatabaseId.of(projectId.get(), instanceId.get(), databaseId.get()));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String query = c.element();
                final String timestampBoundString = this.timestampBound.get();
                log.info(String.format("Received query [%s], timestamp bound [%s]", query, timestampBoundString));
                final Statement statement = Statement.of(query);
                final PartitionOptions options = PartitionOptions.newBuilder()
                        //.setMaxPartitions(10000) // Note: this hint is currently ignored in v1.
                        //.setPartitionSizeBytes(100000000) // Note: this hint is currently ignored in v1.
                        .build();

                final TimestampBound tb;
                if(timestampBoundString == null) {
                    tb = TimestampBound.strong();
                } else {
                    final Instant instant = Instant.parse(timestampBoundString);
                    final com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.ofTimeMicroseconds(instant.getMillis() * 1000);
                    tb = TimestampBound.ofReadTimestamp(timestamp);
                }

                final BatchReadOnlyTransaction transaction = this.batchClient.batchReadOnlyTransaction(tb);
                final List<Partition> partitions = transaction.partitionQuery(options, statement);
                log.info(String.format("Query [%s] (with timestamp bound [%s]) divided to [%d] partitions.", query, tb, partitions.size()));
                for(final Partition partition : partitions) {
                    c.output(KV.of(transaction.getBatchTransactionId(), partition));
                }
            }

            @Teardown
            public void teardown() throws Exception {
                this.spanner.close();
            }

        }

        public class ReadStructSpannerDoFn extends DoFn<KV<BatchTransactionId, Partition>, Struct> {

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
            public void setup() throws Exception {
                final SpannerOptions options = getDefaultInstance();
                this.spanner = options.getService();
                this.batchClient = spanner.getBatchClient(
                        DatabaseId.of(projectId.get(), instanceId.get(), databaseId.get()));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final KV<BatchTransactionId, Partition> kv = c.element();
                final ResultSet resultSet = this.batchClient.batchReadOnlyTransaction(kv.getKey()).execute(kv.getValue());
                int count = 0;
                while(resultSet.next()) {
                    c.output(resultSet.getCurrentRowAsStruct());
                    count++;
                }
                log.info(String.format("Partition query read record num [%d]", count));
            }

            @Teardown
            public void teardown() throws Exception {
                this.spanner.close();
            }

        }

    }

    public static class ReadSingleQuery extends PTransform<PBegin, PCollection<Struct>> {

        private final ValueProvider<String> projectId;
        private final ValueProvider<String> instanceId;
        private final ValueProvider<String> databaseId;
        private final ValueProvider<String> query;
        private final ValueProvider<String> timestampBound;

        private ReadSingleQuery(ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> databaseId,
                                ValueProvider<String> query, ValueProvider<String> timestampBound) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.query = query;
            this.timestampBound = timestampBound;
        }

        public PCollection<Struct> expand(PBegin begin) {
            return begin.getPipeline()
                    .apply("SupplyQuery", Create.ofProvider(this.query, StringUtf8Coder.of()))
                    .apply("ExecuteQuery", ParDo.of(new QuerySpannerDoFn(this.projectId, this.instanceId, this.databaseId, this.timestampBound)));
        }

        public class QuerySpannerDoFn extends DoFn<String, Struct> {

            private final Logger log = LoggerFactory.getLogger(QuerySpannerDoFn.class);

            private final ValueProvider<String> projectId;
            private final ValueProvider<String> instanceId;
            private final ValueProvider<String> databaseId;
            private final ValueProvider<String> timestampBound;

            private Spanner spanner;
            private DatabaseClient client;

            private QuerySpannerDoFn(ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> databaseId, ValueProvider<String> timestampBound) {
                this.projectId = projectId;
                this.instanceId = instanceId;
                this.databaseId = databaseId;
                this.timestampBound = timestampBound;
            }

            @Setup
            public void setup() throws Exception {
                final SpannerOptions options = getDefaultInstance();
                this.spanner = options.getService();
                this.client = spanner.getDatabaseClient(DatabaseId.of(projectId.get(), instanceId.get(), databaseId.get()));
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

                final ResultSet resultSet = this.client.singleUseReadOnlyTransaction(tb).executeQuery(statement);
                log.info(String.format("Query [%s] (with timestamp bound [%s]).", query, tb));
                int count = 0;
                while(resultSet.next()) {
                    c.output(resultSet.getCurrentRowAsStruct());
                    count++;
                }
                log.info(String.format("Query read record num [%d]", count));
            }

            @Teardown
            public void teardown() throws Exception {
                this.spanner.close();
            }

        }

    }


    public static class Write extends PTransform<PCollection<MutationGroup>, SpannerWriteResult> {

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final int limit;

        private final TupleTag<Void> mainTag = new TupleTag<>("mainOut");
        private final TupleTag<MutationGroup> failedTag = new TupleTag<>("failedMutations");

        public SpannerWriteResult expand(PCollection<MutationGroup> mutations) {
            final PCollectionTuple result = mutations
                    .apply("As", ParDo.of(new WriteSpannerDoFn(this.projectId, this.instanceId, this.databaseId, this.failedTag, this.limit)).withOutputTags(mainTag, TupleTagList.of(failedTag)));

            final PCollection<MutationGroup> failedMutations = result.get(failedTag);
            failedMutations.setCoder(SerializableCoder.of(MutationGroup.class));
            return new SpannerWriteResult(mutations.getPipeline(), result.get(mainTag), failedMutations, failedTag);
        }

        public class WriteSpannerDoFn extends DoFn<MutationGroup, Void> {

            private final Logger log = LoggerFactory.getLogger(WriteSpannerDoFn.class);

            private final String projectId;
            private final String instanceId;
            private final String databaseId;
            private final TupleTag<MutationGroup> failedTag;
            private final int limit;

            private Spanner spanner;
            private DatabaseClient databaseClient;
            private BatchClient batchClient;
            private DatabaseAdminClient databaseAdminClient;

            private List<MutationGroup> mutations;

            private WriteSpannerDoFn(String projectId, String instanceId, String databaseId, TupleTag<MutationGroup> failedTag, int limit) {
                this.projectId = projectId;
                this.instanceId = instanceId;
                this.databaseId = databaseId;
                this.failedTag = failedTag;
                this.limit = limit;
            }

            @Setup
            public void setup() throws Exception {
                final SpannerOptions options = getDefaultInstance();
                this.spanner = options.getService();
                this.databaseClient = spanner.getDatabaseClient(
                        DatabaseId.of(projectId, instanceId, databaseId));
                this.batchClient = spanner.getBatchClient(
                        DatabaseId.of(projectId, instanceId, databaseId));
                this.databaseAdminClient = spanner.getDatabaseAdminClient();
            }

            @StartBundle
            public void startBundle(StartBundleContext c) {
                this.mutations = new ArrayList<>();
            }

            @FinishBundle
            public void finishBundle(FinishBundleContext c) {
                if(this.mutations.size() > 0) {
                    Iterable<Mutation> ms = Iterables.concat(this.mutations);
                    this.databaseClient.writeAtLeastOnce(ms);
                    this.mutations.clear();
                }
            }

            @Teardown
            public void teardown() throws Exception {
                this.spanner.close();
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                this.mutations.add(c.element());
                if(this.mutations.size() < this.limit) {
                    return;
                }
                try {
                    final Iterable<Mutation> ms = Iterables.concat(this.mutations);
                    this.databaseClient.writeAtLeastOnce(ms);
                    this.mutations.clear();
                } catch (SpannerException e) {
                    log.warn(e.getMessage());
                    for(MutationGroup m : this.mutations) {
                        c.output(failedTag, m);
                    }
                }
            }
        }

        private Write(String projectId, String instanceId, String databaseId, int limit) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.limit = limit;
        }

    }

}
