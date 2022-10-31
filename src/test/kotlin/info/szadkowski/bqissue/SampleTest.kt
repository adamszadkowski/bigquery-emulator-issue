package info.szadkowski.bqissue

import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isTrue

class SampleTest {

    @Test
    fun `do nothing`() {
        expectThat(true).isTrue()
    }
}
