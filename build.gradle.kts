plugins {
    java
    id("com.gradleup.shadow") version "9.4.1"
    id("com.netflix.nebula.release") version "21.0.0"
}

group = "com.example.lakehouse"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

val scalaVersion = "2.12"
val sparkVersion = "3.5.8"

configurations {
    // Expose compileOnly (Spark cluster libs) to the test classpath
    testImplementation {
        extendsFrom(configurations.compileOnly.get())
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Provided by the Spark cluster — not bundled in the shadow JAR
    compileOnly("org.apache.spark:spark-sql_${scalaVersion}:${sparkVersion}")
    compileOnly("org.apache.spark:spark-sql-kafka-0-10_${scalaVersion}:${sparkVersion}")

    // Bundled in the shadow JAR
    implementation("io.delta:delta-spark_${scalaVersion}:3.3.0")
    implementation("org.apache.hadoop:hadoop-aws:3.3.4")
    implementation("com.amazonaws:aws-java-sdk-bundle:1.12.262")
    implementation("org.slf4j:slf4j-api:2.0.17")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.33")
    implementation("jakarta.validation:jakarta.validation-api:3.0.2")
    implementation("org.hibernate.validator:hibernate-validator:8.0.2.Final")
    implementation("org.apache.avro:avro:1.11.3")
    implementation("io.micrometer:micrometer-registry-prometheus:1.15.1")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.0")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testImplementation("org.mockito:mockito-core:5.23.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.23.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 17
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )
}

tasks.shadowJar {
    archiveClassifier = "all"
    isZip64 = true
    manifest {
        attributes["Main-Class"] = "com.example.lakehouse.IngestionJob"
    }

    // Strip signed-JAR manifests that cause SecurityException when classes are merged
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "META-INF/*.EC")
    mergeServiceFiles()

    // Spark is compileOnly so it won't appear in the shadow JAR; these guards handle
    // any transitive re-introduction (e.g., from delta-spark's optional deps)
    dependencies {
        exclude(dependency("org.apache.spark:.*:.*"))
        exclude(dependency("org.apache.hadoop:hadoop-common:.*"))
        exclude(dependency("org.apache.hadoop:hadoop-client:.*"))
        exclude(dependency("org.apache.hadoop:hadoop-client-api:.*"))
        exclude(dependency("org.apache.hadoop:hadoop-client-runtime:.*"))
        exclude(dependency("org.apache.hadoop:hadoop-hdfs:.*"))
        exclude(dependency("org.apache.hadoop:hadoop-yarn-api:.*"))
        exclude(dependency("org.apache.hadoop:hadoop-yarn-common:.*"))
        exclude(dependency("org.apache.hadoop:hadoop-yarn-client:.*"))
        exclude(dependency("org.apache.hadoop:hadoop-mapreduce-client-core:.*"))
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
