package info.szadkowski.bqissue.utils

import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import java.util.*

class RandomExtension : ParameterResolver {
    override fun supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext?): Boolean =
        parameterContext.parameter.annotatedType.type.equals(String::class.java) &&
                parameterContext.isAnnotated(RandomResolve::class.java)

    override fun resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext?): Any {
        val prefix = parameterContext.findAnnotation(RandomResolve::class.java).get().prefix
        val random = UUID.randomUUID().toString().replace("-", "")
        return "$prefix$random"
    }
}

annotation class RandomResolve(
    val prefix: String = ""
)
