plugins {
    id("com.github.maiflai.scalatest") version "0.32"
    scala
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12:2.4.8")
    implementation("org.apache.spark:spark-sql_2.12:2.4.8")

    testImplementation("org.scalatest:scalatest_2.12:3.2.14")
    testImplementation("com.vladsch.flexmark:flexmark-all:0.64.0")
    testImplementation("org.pegdown:pegdown:1.6.0")
}

tasks.withType<ScalaCompile> {
    targetCompatibility = ""
}
