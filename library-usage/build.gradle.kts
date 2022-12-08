plugins {
    kotlin("jvm") version "1.7.20"
}

dependencies {
    implementation(platform("org.junit:junit-bom:5.9.1"))
    implementation(platform("com.google.cloud:libraries-bom:26.1.5"))

    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))

    implementation("com.google.cloud:google-cloud-bigquery")
    implementation("com.google.cloud:google-cloud-bigquerystorage")
    implementation("org.apache.avro:avro:1.11.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("io.strikt:strikt-core:0.34.1")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks {
    withType<Test> {
        useJUnitPlatform()
    }
}
