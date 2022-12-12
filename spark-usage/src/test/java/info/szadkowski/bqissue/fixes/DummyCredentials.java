package info.szadkowski.bqissue.fixes;

import com.google.cloud.spark.bigquery.repackaged.com.google.auth.Credentials;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class DummyCredentials extends Credentials {
    @Override
    public String getAuthenticationType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
        return emptyMap();
    }

    @Override
    public boolean hasRequestMetadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRequestMetadataOnly() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refresh() {
        throw new UnsupportedOperationException();
    }
}
