package info.szadkowski.bqissue

import com.google.auth.Credentials
import java.net.URI

class DummyCredentials : Credentials() {
    override fun getAuthenticationType(): String {
        TODO("Not yet implemented")
    }

    override fun getRequestMetadata(uri: URI?): MutableMap<String, MutableList<String>> {
        return mutableMapOf()
    }

    override fun hasRequestMetadata(): Boolean {
        TODO("Not yet implemented")
    }

    override fun hasRequestMetadataOnly(): Boolean {
        TODO("Not yet implemented")
    }

    override fun refresh() {
        TODO("Not yet implemented")
    }
}
