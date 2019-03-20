package net.orfeon.cloud.dataflow.templates;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageOptions;
import net.orfeon.cloud.dataflow.transforms.SpannerQueryIO;
import net.orfeon.cloud.dataflow.transforms.StructToAvroTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class SpannerToAvro {

    public interface SpannerToAvroPipelineOption extends PipelineOptions {

        @Description("Project id spanner instance belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id you want to access")
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id you want to access")
        ValueProvider<String> getDatabaseId();
        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("GCS path to output. prefix must start with gs://")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> output);

        @Description("Field key to separate output path")
        ValueProvider<String> getFieldKey();
        void setFieldKey(ValueProvider<String> fieldKey);

        @Description("Use snappy or default codec")
        @Default.Boolean(true)
        ValueProvider<Boolean> getUseSnappy();
        void setUseSnappy(ValueProvider<Boolean> useSnappy);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);

        @Description("(Optional) GCS path to notify job completed.")
        ValueProvider<String> getNotifyFinishGCS();
        void setNotifyFinishGCS(ValueProvider<String> notifyFinishGCS);

        @Description("(Optional) Output empty file even when no query results.")
        @Default.Boolean(false)
        ValueProvider<Boolean> getOutputEmptyFile();
        void setOutputEmptyFile(ValueProvider<Boolean> outputEmptyFile);
    }

    public static void main(String[] args) {

        final SpannerToAvroPipelineOption options = PipelineOptionsFactory.fromArgs(args).as(SpannerToAvroPipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        final WriteFilesResult<String> writeFilesResult = pipeline.apply("QuerySpanner", SpannerQueryIO.read(
                        options.getProjectId(),
                        options.getInstanceId(),
                        options.getDatabaseId(),
                        options.getQuery(),
                        options.getTimestampBound()))
                .apply("StoreGCSAvro", new StructToAvroTransform(
                        options.getOutput(),
                        options.getFieldKey(),
                        options.getUseSnappy()));

        final ValueProvider<String> output = options.getOutput();
        final ValueProvider<Boolean> outputEmptyFile = options.getOutputEmptyFile();
        final ValueProvider<String> notifyFinishGCS = options.getNotifyFinishGCS();

        writeFilesResult.getPerDestinationOutputFilenames()
                .apply("CountResultFiles", Count.globally())
                .apply("PostProcess", ParDo.of(new DoFn<Long, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        if(outputEmptyFile.get() && c.element() == 0) {
                            StorageOptions.getDefaultInstance().getService()
                                    .create(BlobInfo.newBuilder(toBlobId(output.get() + "-empty")).build());
                        }
                        if(notifyFinishGCS.get() != null) {
                            StorageOptions.getDefaultInstance().getService()
                                    .create(BlobInfo.newBuilder(toBlobId(notifyFinishGCS.get())).build());
                        }
                    }

                    private BlobId toBlobId(String gcsPath) {
                        final String[] paths = gcsPath.replaceAll("gs://", "").split("/", 2);
                        return BlobId.of(paths[0], paths[1]);
                    }
                }));

        pipeline.run();
    }

}
