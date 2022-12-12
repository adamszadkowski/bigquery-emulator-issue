plugins {
    id("com.github.maiflai.scalatest") version "0.32"
    scala
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12:2.4.8")
    implementation("org.apache.spark:spark-mllib_2.12:2.4.8")
    implementation("org.apache.spark:spark-sql_2.12:2.4.8")

    implementation("com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1")

    testImplementation("org.scalatest:scalatest_2.12:3.2.14")
    testImplementation("com.vladsch.flexmark:flexmark-all:0.64.0")
    testImplementation("org.pegdown:pegdown:1.6.0")
}

tasks.withType<ScalaCompile> {
    targetCompatibility = ""
}
