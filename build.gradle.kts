plugins {
    kotlin("jvm") version "1.7.20"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("org.junit:junit-bom:5.9.1"))

    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))

    implementation("com.google.cloud:google-cloud-bigquery:2.1.9")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("io.strikt:strikt-core:0.34.1")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks {
    withType<Test> {
        useJUnitPlatform()
    }
}
