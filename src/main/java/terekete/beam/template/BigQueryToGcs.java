package terekete.beam.template;

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class BigQueryToGcs {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryToGcs.class);
  private static final String FILE_SUFFIX = ".avro";

  private static Schema getTableSchema(Storage.ReadSession session) {
    Schema avroSchema;
    avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
    LOG.info("Schema for export is: " + avroSchema.toString());

    return avroSchema;
  }

  public static void main(String[] args) {
    BigQueryToGcsOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(BigQueryToGcsOptions.class);

    run(options);
  }

  private static PipelineResult run(BigQueryToGcsOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    ReadOptions.TableReadOptions.Builder builder = ReadOptions.TableReadOptions.newBuilder();

    if (options.getFields() != null) {
      builder.addAllSelectedFields(Arrays.asList(options.getFields().split(",\\s*")));
    }

    ReadOptions.TableReadOptions tableReadOptions = builder.build();
    BigQueryStorageClient client = BigQueryStorageClientFactory.create();
    Storage.ReadSession session = ReadSessionFactory.create(client, options.getTable(), tableReadOptions);

    Schema schema = getTableSchema(session);
    client.close();

    pipeline
        .apply("ReadFromBQ",
            BigQueryIO.read(SchemaAndRecord::getRecord)
                .from(options.getTable())
                .withTemplateCompatibility()
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                .withCoder(AvroCoder.of(schema))
                .withReadOptions(tableReadOptions))
        .apply("WriteToGcs",
            AvroIO.writeGenericRecords(schema)
                .to(options.getBucket())
                .withNumShards(options.getNumShards())
                .withSuffix(FILE_SUFFIX));
    return pipeline.run();
  }

  public interface BigQueryToGcsOptions extends PipelineOptions {
    @Description("BigQuery table to export from in the form <project>:<dataset>.<table>")
    @Validation.Required
    String getTable();

    void setTable(String table);

    @Description("GCS bucket to export BigQuery table data to (eg: gs://my-bucket/folder/)")
    @Validation.Required
    String getBucket();

    void setBucket(String bucket);

    @Description("Optional: Number of shards for output file")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @Description("Optional: Comma separated list of fields to select from the table")
    String getFields();

    void setFields(String fields);
  }

  static class BigQueryStorageClientFactory {
    static BigQueryStorageClient create() {
      try {
        return BigQueryStorageClient.create();
      } catch (IOException e) {
        LOG.error("Error connecting to BigQueryStorage API: " + e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }

  static class ReadSessionFactory {
    static Storage.ReadSession create(
        BigQueryStorageClient client,
        String tableString,
        ReadOptions.TableReadOptions tableReadOptions) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(tableString);
      String parentProjectId = "projects/" + tableReference.getProjectId();

      TableReferenceProto.TableReference storageTableRef =
          TableReferenceProto.TableReference.newBuilder()
              .setProjectId(tableReference.getProjectId())
              .setDatasetId(tableReference.getDatasetId())
              .setTableId(tableReference.getTableId())
              .build();

      Storage.CreateReadSessionRequest.Builder builder =
          Storage.CreateReadSessionRequest.newBuilder()
              .setParent(parentProjectId)
              .setReadOptions(tableReadOptions)
              .setTableReference(storageTableRef);

      try {
        return client.createReadSession(builder.build());
      } catch (InvalidArgumentException iae) {
        LOG.error("Error creating ReadSession: " + iae.getMessage());
        throw new RuntimeException(iae);
      }
    }
  }
}
