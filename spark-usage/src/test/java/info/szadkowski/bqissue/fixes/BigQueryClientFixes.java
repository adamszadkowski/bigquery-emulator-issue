package info.szadkowski.bqissue.fixes;

import com.google.cloud.spark.bigquery.repackaged.com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.spark.bigquery.repackaged.io.grpc.ManagedChannelBuilder;

import java.util.Optional;

public class BigQueryClientFixes {
    public static final BigQueryClientFixes INSTANCE = new BigQueryClientFixes();

    private BigQueryClientFixes() {
    }

    public void modifyTransportBuilder(InstantiatingGrpcChannelProvider.Builder transportBuilder, Optional<String> endpoint) {
        if (endpoint.isPresent()) {
            transportBuilder.setChannelConfigurator(ManagedChannelBuilder::usePlaintext);
            transportBuilder.setCredentials(new DummyCredentials());
        }
    }

    public void modifyClientSettings(BigQueryReadSettings.Builder clientSettings, Optional<String> endpoint) {
        if (endpoint.isPresent()) {
            clientSettings.setEndpoint(endpoint.get());
            clientSettings.setCredentialsProvider(DummyCredentials::new);
        }
    }
}
