package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.data.DataFrameBuilder
import com.example.dataprocessingexperiment.spark.statistics.StatisticRepository
import com.example.dataprocessingexperiment.spark.statistics.StatisticsRunner
import com.example.dataprocessingexperiment.spark.statistics.collectors.SparkCollector
import com.example.dataprocessingexperiment.tables.Sources
import com.example.dataprocessingexperiment.tables.statistics.Statistics
import com.example.dataprocessingexperiment.tables.statistics.StatisticsConfiguration
import io.github.xn32.json5k.decodeFromStream
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.io.File

/**
 *
 */

class PersistSpike {
    private val logger = KotlinLogging.logger {}

    fun go() {
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // load configuration
        val defaultJsonSerializer = DefaultJsonSerializer()

        val sources = defaultJsonSerializer.tableModule().decodeFromStream<Sources>(
            File("./data/part17/part17.tables.json5").inputStream()
        )

        val outputPath = "./build/output/part17/raw"
        File(outputPath).deleteRecursively()

        val stopWatch = StopWatch.createStarted()

        sparkSession.use {

            val source = sources.sources.first()
            val stopWatchSource = StopWatch.createStarted()

            println("stage 0")

            val rawDataset = sparkSession.read()
                .format(source.type.format)
                .option("header", true) // headers are always required at this point
                .option("delimiter", source.table.delimiter)
//                .option("inferSchema", "true") // infer schema causes another read of the file
                .load("./data/part17/${source.path}")
                .alias(source.name)

            // FileScanRDD

            println("stage 1")
            val rawDataset2 = rawDataset.select("*").sort("total_spend")

            println("stage 2")
            val rawDataset3 = rawDataset2.select("total_spend")
//            println( rawDataset3.toJavaRDD().toDebugString() ) // causes another read of the file

            println("stage 3")
//            rawDataset2.show(10) // causes another read of the file

            // FileScanRDD

            println("stage 4")
            rawDataset2.write().format("csv").save(outputPath) // causes another read of the file

            // FileScanRDD
            // FileScanRDD

            println("stage 5")
            logger.info { "${source.id} took $stopWatchSource" }

        }
        logger.info { "Took $stopWatch" }

    }

}

fun main() {
    println("Starting...")

    PersistSpike().go()

    println("Finished...")
}

/*
/Users/paulrule/.sdkman/candidates/java/17.0.7-tem/bin/java --add-exports=java.base/java.lang=ALL-UNNAMED --add-exports=java.base/java.lang.invoke=ALL-UNNAMED --add-exports=java.base/java.lang.reflect=ALL-UNNAMED --add-exports=java.base/java.io=ALL-UNNAMED --add-exports=java.base/java.net=ALL-UNNAMED --add-exports=java.base/java.nio=ALL-UNNAMED --add-exports=java.base/java.util=ALL-UNNAMED --add-exports=java.base/java.util.concurrent=ALL-UNNAMED --add-exports=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.nio.cs=ALL-UNNAMED --add-exports=java.base/sun.security.action=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED -javaagent:/Users/paulrule/Applications/IntelliJ IDEA Community Edition.app/Contents/lib/idea_rt.jar=65011:/Users/paulrule/Applications/IntelliJ IDEA Community Edition.app/Contents/bin -Dfile.encoding=UTF-8 -classpath /Users/paulrule/IdeaProjects/data-processing-experiment-2/app/build/classes/kotlin/main:/Users/paulrule/IdeaProjects/data-processing-experiment-2/app/build/resources/main:/Users/paulrule/IdeaProjects/data-processing-experiment-2/spark/build/classes/kotlin/main:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.github.microutils/kotlin-logging-jvm/3.0.5/82f2256aeedccfd9c27ea585274a50bf06517383/kotlin-logging-jvm-3.0.5.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.jetbrains.kotlin/kotlin-stdlib/1.9.20/e58b4816ac517e9cc5df1db051120c63d4cde669/kotlin-stdlib-1.9.20.jar:/Users/paulrule/IdeaProjects/data-processing-experiment-2/tables/build/classes/kotlin/main:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-sql_2.13/3.5.0/9a8db650f6f19d38c7be1929924b8fea3e875a63/spark-sql_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-core/2.15.2/a6fe1836469a69b3ff66037c324d75fc66ef137c/jackson-core-2.15.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.scala-lang/scala-library/2.13.12/3c51da898af4ebfac356fcde11388afb2ae8ee82/scala-library-2.13.12.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.jetbrains.kotlin/kotlin-stdlib-jdk8/1.8.10/7c002ac41f547a82e81dfebd2a20577a738dbf3f/kotlin-stdlib-jdk8-1.8.10.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-api/2.0.7/41eb7184ea9d556f23e18b5cb99cad1f8581fc00/slf4j-api-2.0.7.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.github.xn32/json5k-jvm/0.3.0/8cfd58860563a71a9397fbb292299f7fc5ca04bd/json5k-jvm-0.3.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.jetbrains/annotations/17.0.0/8ceead41f4e71821919dbdb7a9847608f1a938cb/annotations-17.0.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.orc/orc-core/1.9.1/67ba092497265fb74a2884831253f434547da1bf/orc-core-1.9.1-shaded-protobuf.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-catalyst_2.13/3.5.0/9dfe44ad2aca4a4c57ec2493f3b4c61c691c700e/spark-catalyst_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-core_2.13/3.5.0/5e566f228cf4bac6e885e1d0eaa00b6ce27a1606/spark-core_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-sketch_2.13/3.5.0/da4e32e45166766dac98d1f5318eb5764bc75602/spark-sketch_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-tags_2.13/3.5.0/665f58a209c43167555d25fbd9ddc3268c74b3bf/spark-tags_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.scala-lang.modules/scala-parallel-collections_2.13/1.0.4/9846d1ed01f24e90d7f9495987033b8274835431/scala-parallel-collections_2.13-1.0.4.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-databind/2.15.2/9353b021f10c307c00328f52090de2bdb4b6ff9c/jackson-databind-2.15.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.hive/hive-storage-api/2.8.1/4b151bcdfe290542f27a442ed09be99f815f88e8/hive-storage-api-2.8.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.parquet/parquet-hadoop/1.13.1/6b9a84875b7576d64cf4890c69c2ed1c7557b950/parquet-hadoop-1.13.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.parquet/parquet-column/1.13.1/9ed76ae5d9fdfc6c2eb51aef87ad34c5cdc56384/parquet-column-1.13.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.rocksdb/rocksdbjni/8.3.2/58691480ac749ec2b12b1f3ea9941ac9f7d5a361/rocksdbjni-8.3.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.univocity/univocity-parsers/2.9.1/81827d186e42129f23c3f1e002b757ad4b4e769/univocity-parsers-2.9.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.orc/orc-mapreduce/1.9.1/1d5732ef544f2c1101475052501ffe391ccbac06/orc-mapreduce-1.9.1-shaded-protobuf.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.xbean/xbean-asm9-shaded/4.23/14e59bc47394bef27a6437074dc9178740832313/xbean-asm9-shaded-4.23.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.jetbrains.kotlin/kotlin-stdlib-jdk7/1.8.10/cb726a23c808a850a43e7d6b9d1ba91b02fe9f05/kotlin-stdlib-jdk7-1.8.10.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.orc/orc-shims/1.9.1/b0345540c6db1f7b34db5edb25bac5c66068aa73/orc-shims-1.9.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-lang3/3.12.0/c6842c86792ff03b9f1d1fe2aab8dc23aa6c6f0e/commons-lang3-3.12.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.airlift/aircompressor/0.25/f9335fb25f31289ffbe512aa0732c872abdfd64d/aircompressor-0.25.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.threeten/threeten-extra/1.7.1/caddbd1f234f87b0d65e421b5d5032b6a473f67b/threeten-extra-1.7.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-sql-api_2.13/3.5.0/3706745cc0097089eea3cb1c8c1a8ab31a6fdd9b/spark-sql-api_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-unsafe_2.13/3.5.0/42d5ce858976bed0bed2e3ef394a173cfc516f9b/spark-unsafe_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/commons-codec/commons-codec/1.16.0/4e3eb3d79888d76b54e28b350915b5dc3919c9de/commons-codec-1.16.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.codehaus.janino/janino/3.1.9/536fb0c44627faae32ca7a8a24734f4aab38c878/janino-3.1.9.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.codehaus.janino/commons-compiler/3.1.9/f0d70bb319e9339aea90a8665693e69848acc598/commons-compiler-3.1.9.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.datasketches/datasketches-java/3.3.0/fcf28150084f55866315283d9f25e500a05f6a20/datasketches-java-3.3.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-launcher_2.13/3.5.0/97541faeb7dfb6375b801c4356ba916e67178f66/spark-launcher_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-kvstore_2.13/3.5.0/260139eeaa29f95d80e9e68a7bf8511cfc8bef59/spark-kvstore_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-network-shuffle_2.13/3.5.0/7c5d8396bf01ed6eaad7c392f44d1b11905634ed/spark-network-shuffle_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-network-common_2.13/3.5.0/1a98431912872af4ecf9940c05ad8b55f3f7114d/spark-network-common_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.spark/spark-common-utils_2.13/3.5.0/e9526b5ba70e887b6d72172be5f15c6b1b3ac723/spark-common-utils_2.13-3.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.twitter/chill_2.13/0.10.0/764f2844ce1c7033869e069dc7cfe3f9682d175f/chill_2.13-0.10.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.scala-lang.modules/scala-xml_2.13/2.1.0/f2c107c097692b6c07941e7b96fcf6ec3b58add6/scala-xml_2.13-2.1.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.scala-lang/scala-reflect/2.13.8/994b004d041b18724ec298a135c37e7817d369ec/scala-reflect-2.13.8.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.json4s/json4s-jackson_2.13/3.7.0-M11/69fcabd60bca85fecb4756db1d5f18ccfdabad4b/json4s-jackson_2.13-3.7.0-M11.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.avro/avro-mapred/1.11.2/2005a729189450b43092633ad1a5d7379a901585/avro-mapred-1.11.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.avro/avro/1.11.2/97e62e8be2b37e849f1bdb5a4f08121d47cc9806/avro-1.11.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-json/4.2.19/32afaab5bca42c8cbd4002f906b169d6c0fd4e4f/metrics-json-4.2.19.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.module/jackson-module-scala_2.13/2.15.2/405653902e735adbd95c323f68e6a787f0c55c45/jackson-module-scala_2.13-2.15.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.clearspring.analytics/stream/2.9.6/f9c235bdf6681756b8d4b5429f6e7217597c37ef/stream-2.9.6.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-jvm/4.2.19/13bb7b51914d43a7709f7b477b98f3732aaa03e4/metrics-jvm-4.2.19.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-graphite/4.2.19/c8ed09f56bca464c136066bedbac86f451aa7f20/metrics-graphite-4.2.19.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-jmx/4.2.19/474c1c764df01a0d93fc268da1f779ab97fe99ae/metrics-jmx-4.2.19.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-core/4.2.19/d32b4c3f3e733bf4cb239ca4204fbed8464973a5/metrics-core-4.2.19.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.twitter/chill-java/0.10.0/fd2eb52afd9ab4337c9e51823f41ad8916e6976/chill-java-0.10.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.hadoop/hadoop-client-api/3.3.4/6339a8f7279310c8b1f7ef314b592d8c71ca72ef/hadoop-client-api-3.3.4.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.hadoop/hadoop-client-runtime/3.3.4/21f7a9a2da446f1e5b3e5af16ebf956d3ee43ee0/hadoop-client-runtime-3.3.4.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/javax.activation/activation/1.1.1/485de3a253e23f645037828c07f1d7f1af40763a/activation-1.1.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.curator/curator-recipes/2.13.0/1e6d5cf7b18a402f5d52785877010711538d68a0/curator-recipes-2.13.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.zookeeper/zookeeper/3.6.3/a6e74f826db85ff8c51c15ef0fa2ea0b462aef25/zookeeper-3.6.3.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/jakarta.servlet/jakarta.servlet-api/4.0.3/7c810f7bca93d109bac3323286b8e5ec6c394e12/jakarta.servlet-api-4.0.3.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-compress/1.23.0/4af2060ea9b0c8b74f1854c6cafe4d43cfc161fc/commons-compress-1.23.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-text/1.10.0/3363381aef8cef2dbc1023b3e3a9433b08b64e01/commons-text-1.10.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-math3/3.6.1/e4ba98f1d4b3c80ec46392f25e094a6a2e58fcbf/commons-math3-3.6.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/commons-io/commons-io/2.13.0/8bb2bc9b4df17e2411533a0708a69f983bf5e83b/commons-io-2.13.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/commons-collections/commons-collections/3.2.2/8ad72fe39fa8c91eaaf12aadb21e0c3661fe26d5/commons-collections-3.2.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-collections4/4.4/62ebe7544cb7164d87e0637a2a6a2bdc981395e8/commons-collections4-4.4.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.google.code.findbugs/jsr305/3.0.2/25ea2e8b0c338a877313bd4672d3fe056ea78f0d/jsr305-3.0.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.ning/compress-lzf/1.1.2/d09d33ffa7bc1d987db92e5ebec926ff92b7cbdf/compress-lzf-1.1.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.xerial.snappy/snappy-java/1.1.10.3/4548ee2aac847998146e8d4a3176f7bcc766a00/snappy-java-1.1.10.3.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.lz4/lz4-java/1.8.0/4b986a99445e49ea5fbf5d149c4b63f6ed6c6780/lz4-java-1.8.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.github.luben/zstd-jni/1.5.5-4/d724d95eb8423d185e2efb8ba15f7f3fdf200cfb/zstd-jni-1.5.5-4.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.roaringbitmap/RoaringBitmap/0.9.45/856885b6afb1cad048dd188eb126e16dc47dd713/RoaringBitmap-0.9.45.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.jersey.containers/jersey-container-servlet/2.40/a06f7096c476fd127a22f18fe767c5175f4b4dbf/jersey-container-servlet-2.40.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.jersey.containers/jersey-container-servlet-core/2.40/b1c171b170fc0b062f04842991de043765c84ec2/jersey-container-servlet-core-2.40.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.jersey.core/jersey-server/2.40/4550ae3be0d3e0434144c2e9ae9ef46041571b1b/jersey-server-2.40.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.jersey.core/jersey-client/2.40/231decdfc01b6c73411af2577a502c19632a1ed0/jersey-client-2.40.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.jersey.inject/jersey-hk2/2.40/4c94775bdcce0c2e32b231f6c638e82bfe3b6eee/jersey-hk2-2.40.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.jersey.core/jersey-common/2.40/b826a4ebfcf316b7f168a41dc07657f0b889e6a3/jersey-common-2.40.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-all/4.1.96.Final/2145ec747511965e4a57099767654cf9083ce8a7/netty-all-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-transport-native-epoll/4.1.96.Final/9faf365396c933f1b39b60d129391c6c6c43fb86/netty-transport-native-epoll-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-transport-native-epoll/4.1.96.Final/cae778ab150745432a90d4f26f6174fe564f56fc/netty-transport-native-epoll-4.1.96.Final-linux-aarch_64.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-transport-native-epoll/4.1.96.Final/3f8904e072cfc9a8d67c6fe567c39bcbce5c9c55/netty-transport-native-epoll-4.1.96.Final-linux-x86_64.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-transport-native-kqueue/4.1.96.Final/70b3e957eec0cd78637c3bd15a8a4b24e653f87/netty-transport-native-kqueue-4.1.96.Final-osx-aarch_64.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-transport-native-kqueue/4.1.96.Final/c127ed313fc80cf2cb366dccfded1daddc89a8ef/netty-transport-native-kqueue-4.1.96.Final-osx-x86_64.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.ivy/ivy/2.5.1/7fac35f24f89776e7b78ec98658d8bc8f22f7e89/ivy-2.5.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/oro/oro/2.0.8/5592374f834645c4ae250f4c9fbb314c9369d698/oro-2.0.8.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/net.razorvine/pickle/1.3/43eab5f4a8d0a06a38a6c349dec32bd08454c176/pickle-1.3.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/net.sf.py4j/py4j/0.10.9.7/e444374109f6f3ffdfdbd4e7dc5a89122b0c9134/py4j-0.10.9.7.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-crypto/1.1.0/4a8b4caa84032a0f1f1dad16875820a4f37524b7/commons-crypto-1.1.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-annotations/2.15.2/4724a65ac8e8d156a24898d50fd5dbd3642870b8/jackson-annotations-2.15.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.parquet/parquet-common/1.13.1/77e65a1a4522f476994aa90c06fcca842c4ab73f/parquet-common-1.13.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.yetus/audience-annotations/0.13.0/8ad1147dcd02196e3924013679c6bf4c25d8c351/audience-annotations-0.13.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.parquet/parquet-format-structures/1.13.1/550ac211af104eabeedfaabe68f6769010a80366/parquet-format-structures-1.13.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.parquet/parquet-encoding/1.13.1/2554cef74a90736ad42ed994116123038c0ffee9/parquet-encoding-1.13.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.jetbrains.kotlinx/kotlinx-serialization-core-jvm/1.5.0/d701e8cccd443a7cc1a0bcac53432f2745dcdbda/kotlinx-serialization-core-jvm-1.5.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.arrow/arrow-vector/12.0.1/809a112926070f912f12c151d5ab1fa810612ca5/arrow-vector-12.0.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.scala-lang.modules/scala-parser-combinators_2.13/2.3.0/40851294d2aedb4304cfa40fa413eb6e944a7968/scala-parser-combinators_2.13-2.3.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.arrow/arrow-memory-netty/12.0.1/f0e6801aa116b75775272dcac6ad49584382bf8c/arrow-memory-netty-12.0.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.antlr/antlr4-runtime/4.9.3/81befc16ebedb8b8aea3e4c0835dd5ca7e8523a8/antlr4-runtime-4.9.3.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.datasketches/datasketches-memory/2.1.0/f9ef215c32b85e331d62a9dbf4bafb405a5f543a/datasketches-memory-2.1.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.fusesource.leveldbjni/leveldbjni-all/1.8/707350a2eeb1fa2ed77a32ddb3893ed308e941db/leveldbjni-all-1.8.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.google.crypto.tink/tink/1.9.0/10de1d0ea8401c3e33cad83c0d11df712d40ba5/tink-1.9.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.slf4j/jul-to-slf4j/2.0.7/a48f44aeaa8a5ddc347007298a28173ac1fbbd8b/jul-to-slf4j-2.0.7.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.slf4j/jcl-over-slf4j/2.0.7/f127fe5ee53404a8b3697cdd032dd1dd6a29dd77/jcl-over-slf4j-2.0.7.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.logging.log4j/log4j-slf4j2-impl/2.20.0/155c8b9bbdac91d8461d9a403a646e6bd0d365d8/log4j-slf4j2-impl-2.20.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.logging.log4j/log4j-core/2.20.0/eb2a9a47b1396e00b5eee1264296729a70565cc0/log4j-core-2.20.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.logging.log4j/log4j-1.2-api/2.20.0/689151374756cb809cb029f2501015bdc7733179/log4j-1.2-api-2.20.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.logging.log4j/log4j-api/2.20.0/1fe6082e660daf07c689a89c94dc0f49c26b44bb/log4j-api-2.20.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.esotericsoftware/kryo-shaded/4.0.2/e8c89779f93091aa9cb895093402b5d15065bf88/kryo-shaded-4.0.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.json4s/json4s-core_2.13/3.7.0-M11/e04ce5f5a27292ff7bb13ced353137e10813a9f2/json4s-core_2.13-3.7.0-M11.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.avro/avro-ipc/1.11.2/45c8d7b32c67ae28ee56b4ca16a74db7332c9402/avro-ipc-1.11.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.thoughtworks.paranamer/paranamer/2.8/619eba74c19ccf1da8ebec97a2d7f8ba05773dd6/paranamer-2.8.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.curator/curator-framework/2.13.0/d45229aee7d3f1f628a34fcac9b66ed5ba52c31f/curator-framework-2.13.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.zookeeper/zookeeper-jute/3.6.3/8990d19ec3db01f45f82d4011a11b085db66de05/zookeeper-jute-3.6.3.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-handler/4.1.96.Final/7840d7523d709e02961b647546f9d9dde1699306/netty-handler-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/jakarta.ws.rs/jakarta.ws.rs-api/2.1.6/1dcb770bce80a490dff49729b99c7a60e9ecb122/jakarta.ws.rs-api-2.1.6.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.hk2.external/jakarta.inject/2.6.1/8096ebf722902e75fbd4f532a751e514f02e1eb7/jakarta.inject-2.6.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/jakarta.annotation/jakarta.annotation-api/1.3.5/59eb84ee0d616332ff44aba065f3888cf002cd2d/jakarta.annotation-api-1.3.5.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/jakarta.validation/jakarta.validation-api/2.0.2/5eacc6522521f7eacb081f95cee1e231648461e7/jakarta.validation-api-2.0.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.hk2/hk2-locator/2.6.1/9dedf9d2022e38ec0743ed44c1ac94ad6149acdd/hk2-locator-2.6.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.javassist/javassist/3.29.2-GA/6c32028609e5dd4a1b78e10fbcd122b09b3928b1/javassist-3.29.2-GA.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.hk2/osgi-resource-locator/1.0.3/de3b21279df7e755e38275137539be5e2c80dd58/osgi-resource-locator-1.0.3.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-transport-classes-epoll/4.1.96.Final/b0369501645f6e71f89ff7f77b5c5f52510a2e31/netty-transport-classes-epoll-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-transport-classes-kqueue/4.1.96.Final/782f6bbb8dd5401599d272ea0fb81d1356bdffb2/netty-transport-classes-kqueue-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-transport-native-unix-common/4.1.96.Final/daf8578cade63a01525ee9d70371fa78e6e91094/netty-transport-native-unix-common-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-codec/4.1.96.Final/9cfe430f8b14e7ba86969d8e1126aa0aae4d18f0/netty-codec-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-transport/4.1.96.Final/dbd15ca244be28e1a98ed29b9d755edbfa737e02/netty-transport-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-buffer/4.1.96.Final/4b80fffbe77485b457bf844289bf1801f61b9e91/netty-buffer-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-codec-http/4.1.96.Final/a4d0d95df5026965c454902ef3d6d84b81f89626/netty-codec-http-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-codec-http2/4.1.96.Final/cc8baf4ff67c1bcc0cde60bc5c2bb9447d92d9e6/netty-codec-http2-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-codec-socks/4.1.96.Final/f53c52dbddaa4a02a51430405792d3f30a89b147/netty-codec-socks-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-resolver/4.1.96.Final/e51db5568a881e0f9b013b35617c597dc32f130/netty-resolver-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-common/4.1.96.Final/d10c167623cbc471753f950846df241d1021655c/netty-common-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/io.netty/netty-handler-proxy/4.1.96.Final/dcabd63f4aaec2b4cad7588bfdd4cd2c82287e38/netty-handler-proxy-4.1.96.Final.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.datatype/jackson-datatype-jsr310/2.15.2/30d16ec2aef6d8094c5e2dce1d95034ca8b6cb42/jackson-datatype-jsr310-2.15.2.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.arrow/arrow-memory-core/12.0.1/cd7f41f4ebbb7cdab55b7a3b935ab5bcb2108b8d/arrow-memory-core-12.0.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.arrow/arrow-format/12.0.1/b2dbacdcafb42e4fd7432468010d5d4d82380778/arrow-format-12.0.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.google.flatbuffers/flatbuffers-java/1.12.0/8201cc7b511177a37071249e891f2f2fea4b32e9/flatbuffers-java-1.12.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.google.code.gson/gson/2.10.1/b3add478d4382b78ea20b1671390a858002feb6c/gson-2.10.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.google.protobuf/protobuf-java/3.19.6/578a2070b8106d3ea350fc737e777262bdc796b1/protobuf-java-3.19.6.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/joda-time/joda-time/2.12.5/698ce67b5e58becfb4ef2cf0393422775e59dff4/joda-time-2.12.5.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.esotericsoftware/minlog/1.3.0/ff07b5f1b01d2f92bb00a337f9a94873712f0827/minlog-1.3.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.objenesis/objenesis/2.5.1/272bab9a4e5994757044d1fc43ce480c8cb907a4/objenesis-2.5.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.json4s/json4s-ast_2.13/3.7.0-M11/5cdee05c25c367092ac5f8f1261989e4e25568e3/json4s-ast_2.13-3.7.0-M11.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.json4s/json4s-scalap_2.13/3.7.0-M11/e225fbf829ede3a4f68519490e0c5b82988bfb8d/json4s-scalap_2.13-3.7.0-M11.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.tukaani/xz/1.9/1ea4bec1a921180164852c65006d928617bd2caf/xz-1.9.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.curator/curator-client/2.13.0/a1974d9b3251c055408059b2f408d19d7db07224/curator-client-2.13.0.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.hk2/hk2-api/2.6.1/114bd7afb4a1bd9993527f52a08a252b5d2acac5/hk2-api-2.6.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.hk2/hk2-utils/2.6.1/396513aa96c1d5a10aa4f75c4dcbf259a698d62d/hk2-utils-2.6.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.glassfish.hk2.external/aopalliance-repackaged/2.6.1/b2eb0a83bcbb44cc5d25f8b18f23be116313a638/aopalliance-repackaged-2.6.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/com.google.guava/guava/16.0.1/5fa98cd1a63c99a44dd8d3b77e4762b066a5d0c5/guava-16.0.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/ch.qos.logback/logback-classic/1.4.14/d98bc162275134cdf1518774da4a2a17ef6fb94d/logback-classic-1.4.14.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/ch.qos.logback/logback-core/1.4.14/4d3c2248219ac0effeb380ed4c5280a80bf395e8/logback-core-1.4.14.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.apache.parquet/parquet-jackson/1.13.1/c096efa2bb68d24d29139e93f9b5a5357f10f253/parquet-jackson-1.13.1.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/commons-logging/commons-logging/1.1.3/f6f66e966c70a83ffbdb6f17a0919eaf7c8aca7f/commons-logging-1.1.3.jar:/Users/paulrule/.gradle/caches/modules-2/files-2.1/org.roaringbitmap/shims/0.9.45/480ae48bcdde25c911aad8d212ba2641fe9b4be9/shims-0.9.45.jar com.example.dataprocessingexperiment.app.PersistSpikeKt
Starting...
SLF4J: Class path contains multiple SLF4J providers.
SLF4J: Found provider [org.apache.logging.slf4j.SLF4JServiceProvider@78e67e0a]
SLF4J: Found provider [ch.qos.logback.classic.spi.LogbackServiceProvider@bd8db5a]
SLF4J: See https://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual provider is of type [org.apache.logging.slf4j.SLF4JServiceProvider@78e67e0a]
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
24/06/02 20:20:32 INFO SparkContext: Running Spark version 3.5.0
24/06/02 20:20:32 INFO SparkContext: OS info Mac OS X, 14.5, aarch64
24/06/02 20:20:32 INFO SparkContext: Java version 17.0.7
24/06/02 20:20:32 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
24/06/02 20:20:32 INFO ResourceUtils: ==============================================================
24/06/02 20:20:32 INFO ResourceUtils: No custom resources configured for spark.driver.
24/06/02 20:20:32 INFO ResourceUtils: ==============================================================
24/06/02 20:20:32 INFO SparkContext: Submitted application: spike
24/06/02 20:20:32 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
24/06/02 20:20:32 INFO ResourceProfile: Limiting resource is cpu
24/06/02 20:20:32 INFO ResourceProfileManager: Added ResourceProfile id: 0
24/06/02 20:20:32 INFO SecurityManager: Changing view acls to: paulrule
24/06/02 20:20:32 INFO SecurityManager: Changing modify acls to: paulrule
24/06/02 20:20:32 INFO SecurityManager: Changing view acls groups to:
24/06/02 20:20:32 INFO SecurityManager: Changing modify acls groups to:
24/06/02 20:20:32 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: paulrule; groups with view permissions: EMPTY; users with modify permissions: paulrule; groups with modify permissions: EMPTY
24/06/02 20:20:32 INFO Utils: Successfully started service 'sparkDriver' on port 65013.
24/06/02 20:20:32 INFO SparkEnv: Registering MapOutputTracker
24/06/02 20:20:32 INFO SparkEnv: Registering BlockManagerMaster
24/06/02 20:20:32 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
24/06/02 20:20:32 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
24/06/02 20:20:32 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
24/06/02 20:20:32 INFO DiskBlockManager: Created local directory at /private/var/folders/5h/vkqskygs6xqgldrbd4nfgzjc0000gn/T/blockmgr-81ece0fa-ae66-4cf7-8856-b206bf68317d
24/06/02 20:20:32 INFO MemoryStore: MemoryStore started with capacity 3.4 GiB
24/06/02 20:20:32 INFO SparkEnv: Registering OutputCommitCoordinator
24/06/02 20:20:32 INFO JettyUtils: Start Jetty 0.0.0.0:4040 for SparkUI
24/06/02 20:20:32 INFO Utils: Successfully started service 'SparkUI' on port 4040.
24/06/02 20:20:32 INFO Executor: Starting executor ID driver on host 192-168-1-168.tpgi.com.au
24/06/02 20:20:32 INFO Executor: OS info Mac OS X, 14.5, aarch64
24/06/02 20:20:32 INFO Executor: Java version 17.0.7
24/06/02 20:20:32 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
24/06/02 20:20:32 INFO Executor: Created or updated repl class loader org.apache.spark.util.MutableURLClassLoader@4f4c88f9 for default.
24/06/02 20:20:32 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 65014.
24/06/02 20:20:32 INFO NettyBlockTransferService: Server created on 192-168-1-168.tpgi.com.au:65014
24/06/02 20:20:32 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
24/06/02 20:20:32 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192-168-1-168.tpgi.com.au, 65014, None)
24/06/02 20:20:32 INFO BlockManagerMasterEndpoint: Registering block manager 192-168-1-168.tpgi.com.au:65014 with 3.4 GiB RAM, BlockManagerId(driver, 192-168-1-168.tpgi.com.au, 65014, None)
24/06/02 20:20:32 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192-168-1-168.tpgi.com.au, 65014, None)
24/06/02 20:20:32 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192-168-1-168.tpgi.com.au, 65014, None)
stage 0
24/06/02 20:20:32 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
24/06/02 20:20:32 INFO SharedState: Warehouse path is 'file:/Users/paulrule/IdeaProjects/data-processing-experiment-2/spark-warehouse'.
24/06/02 20:20:33 INFO InMemoryFileIndex: It took 21 ms to list leaf files for 1 paths.
24/06/02 20:20:33 INFO InMemoryFileIndex: It took 0 ms to list leaf files for 1 paths.
24/06/02 20:20:34 INFO FileSourceStrategy: Pushed Filters:
24/06/02 20:20:34 INFO FileSourceStrategy: Post-Scan Filters: (length(trim(value#0, None)) > 0)
24/06/02 20:20:34 INFO CodeGenerator: Code generated in 89.055334 ms
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 376.0 B, free 3.4 GiB)
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 34.1 KiB, free 3.4 GiB)
24/06/02 20:20:34 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 192-168-1-168.tpgi.com.au:65014 (size: 34.1 KiB, free: 3.4 GiB)
24/06/02 20:20:34 INFO SparkContext: Created broadcast 0 from load at PersistSpike.kt:53
24/06/02 20:20:34 INFO FileSourceScanExec: Planning scan with bin packing, max size: 11601677 bytes, open cost is considered as scanning 4194304 bytes.
24/06/02 20:20:34 INFO SparkContext: Starting job: load at PersistSpike.kt:53
24/06/02 20:20:34 INFO DAGScheduler: Got job 0 (load at PersistSpike.kt:53) with 1 output partitions
24/06/02 20:20:34 INFO DAGScheduler: Final stage: ResultStage 0 (load at PersistSpike.kt:53)
24/06/02 20:20:34 INFO DAGScheduler: Parents of final stage: List()
24/06/02 20:20:34 INFO DAGScheduler: Missing parents: List()
24/06/02 20:20:34 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[3] at load at PersistSpike.kt:53), which has no missing parents
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 13.8 KiB, free 3.4 GiB)
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 6.5 KiB, free 3.4 GiB)
24/06/02 20:20:34 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 192-168-1-168.tpgi.com.au:65014 (size: 6.5 KiB, free: 3.4 GiB)
24/06/02 20:20:34 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1580
24/06/02 20:20:34 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (MapPartitionsRDD[3] at load at PersistSpike.kt:53) (first 15 tasks are for partitions Vector(0))
24/06/02 20:20:34 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
24/06/02 20:20:34 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (192-168-1-168.tpgi.com.au, executor driver, partition 0, PROCESS_LOCAL, 8359 bytes)
24/06/02 20:20:34 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
24/06/02 20:20:34 INFO CodeGenerator: Code generated in 5.956667 ms
24/06/02 20:20:34 INFO FileScanRDD: Reading File path: file:///Users/paulrule/IdeaProjects/data-processing-experiment-2/data/part17/downloaded/addresses.csv, range: 0-7407373, partition values: [empty row]
24/06/02 20:20:34 INFO CodeGenerator: Code generated in 4.518916 ms
24/06/02 20:20:34 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1733 bytes result sent to driver
24/06/02 20:20:34 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 98 ms on 192-168-1-168.tpgi.com.au (executor driver) (1/1)
24/06/02 20:20:34 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool
24/06/02 20:20:34 INFO DAGScheduler: ResultStage 0 (load at PersistSpike.kt:53) finished in 0.151 s
24/06/02 20:20:34 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
24/06/02 20:20:34 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
24/06/02 20:20:34 INFO DAGScheduler: Job 0 finished: load at PersistSpike.kt:53, took 0.173109 s
24/06/02 20:20:34 INFO CodeGenerator: Code generated in 3.902375 ms
24/06/02 20:20:34 INFO FileSourceStrategy: Pushed Filters:
24/06/02 20:20:34 INFO FileSourceStrategy: Post-Scan Filters:
24/06/02 20:20:34 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 192-168-1-168.tpgi.com.au:65014 in memory (size: 6.5 KiB, free: 3.4 GiB)
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 376.0 B, free 3.4 GiB)
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 34.1 KiB, free 3.4 GiB)
24/06/02 20:20:34 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 192-168-1-168.tpgi.com.au:65014 (size: 34.1 KiB, free: 3.4 GiB)
24/06/02 20:20:34 INFO SparkContext: Created broadcast 2 from load at PersistSpike.kt:53
24/06/02 20:20:34 INFO FileSourceScanExec: Planning scan with bin packing, max size: 11601677 bytes, open cost is considered as scanning 4194304 bytes.
stage 1
24/06/02 20:20:34 INFO FileSourceStrategy: Pushed Filters:
24/06/02 20:20:34 INFO FileSourceStrategy: Post-Scan Filters:
24/06/02 20:20:34 INFO CodeGenerator: Code generated in 5.583917 ms
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 376.0 B, free 3.4 GiB)
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 34.1 KiB, free 3.4 GiB)
24/06/02 20:20:34 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 192-168-1-168.tpgi.com.au:65014 (size: 34.1 KiB, free: 3.4 GiB)
24/06/02 20:20:34 INFO SparkContext: Created broadcast 3 from show at PersistSpike.kt:58
24/06/02 20:20:34 INFO FileSourceScanExec: Planning scan with bin packing, max size: 11601677 bytes, open cost is considered as scanning 4194304 bytes.
24/06/02 20:20:34 INFO SparkContext: Starting job: show at PersistSpike.kt:58
24/06/02 20:20:34 INFO DAGScheduler: Got job 1 (show at PersistSpike.kt:58) with 1 output partitions
24/06/02 20:20:34 INFO DAGScheduler: Final stage: ResultStage 1 (show at PersistSpike.kt:58)
24/06/02 20:20:34 INFO DAGScheduler: Parents of final stage: List()
24/06/02 20:20:34 INFO DAGScheduler: Missing parents: List()
24/06/02 20:20:34 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[13] at show at PersistSpike.kt:58), which has no missing parents
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 16.3 KiB, free 3.4 GiB)
24/06/02 20:20:34 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 7.6 KiB, free 3.4 GiB)
24/06/02 20:20:34 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 192-168-1-168.tpgi.com.au:65014 (size: 7.6 KiB, free: 3.4 GiB)
24/06/02 20:20:34 INFO SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:1580
24/06/02 20:20:34 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[13] at show at PersistSpike.kt:58) (first 15 tasks are for partitions Vector(0))
24/06/02 20:20:34 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks resource profile 0
24/06/02 20:20:34 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1) (192-168-1-168.tpgi.com.au, executor driver, partition 0, PROCESS_LOCAL, 8359 bytes)
24/06/02 20:20:34 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
24/06/02 20:20:34 INFO CodeGenerator: Code generated in 8.316875 ms
24/06/02 20:20:34 INFO FileScanRDD: Reading File path: file:///Users/paulrule/IdeaProjects/data-processing-experiment-2/data/part17/downloaded/addresses.csv, range: 0-7407373, partition values: [empty row]
24/06/02 20:20:34 INFO CodeGenerator: Code generated in 4.924125 ms
24/06/02 20:20:34 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 1918 bytes result sent to driver
24/06/02 20:20:34 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 84 ms on 192-168-1-168.tpgi.com.au (executor driver) (1/1)
24/06/02 20:20:34 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool
24/06/02 20:20:34 INFO DAGScheduler: ResultStage 1 (show at PersistSpike.kt:58) finished in 0.090 s
24/06/02 20:20:34 INFO DAGScheduler: Job 1 is finished. Cancelling potential speculative or zombie tasks for this job
24/06/02 20:20:34 INFO TaskSchedulerImpl: Killing all running tasks in stage 1: Stage finished
24/06/02 20:20:34 INFO DAGScheduler: Job 1 finished: show at PersistSpike.kt:58, took 0.093154 s
24/06/02 20:20:34 INFO CodeGenerator: Code generated in 6.42475 ms
24/06/02 20:20:34 INFO PersistSpike: addresses took 00:00:02.153
24/06/02 20:20:34 INFO SparkContext: SparkContext is stopping with exitCode 0.
+---------------+--------------------+-----------+
|     company_id|             address|total_spend|
+---------------+--------------------+-----------+
|              1|        APARTMENT 2,|       NULL|
|52 BEDFORD ROAD|                NULL|       NULL|
|         LONDON|                NULL|       NULL|
|        ENGLAND|                NULL|       NULL|
|       SW4 7HJ"|                5700|       NULL|
|              2|107 SHERINGHAM AV...|       NULL|
|         LONDON|                NULL|       NULL|
|       N14 4UJ"|                4700|       NULL|
|              3|     43 SUNNINGDALE,|       NULL|
|           YATE|                NULL|       NULL|
+---------------+--------------------+-----------+
only showing top 10 rows

stage 2
24/06/02 20:20:34 INFO SparkUI: Stopped Spark web UI at http://192-168-1-168.tpgi.com.au:4040
24/06/02 20:20:34 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
24/06/02 20:20:34 INFO MemoryStore: MemoryStore cleared
24/06/02 20:20:34 INFO BlockManager: BlockManager stopped
24/06/02 20:20:34 INFO BlockManagerMaster: BlockManagerMaster stopped
24/06/02 20:20:34 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
24/06/02 20:20:34 INFO SparkContext: Successfully stopped SparkContext
24/06/02 20:20:34 INFO PersistSpike: Took 00:00:02.180
24/06/02 20:20:34 INFO ShutdownHookManager: Shutdown hook called
24/06/02 20:20:34 INFO ShutdownHookManager: Deleting directory /private/var/folders/5h/vkqskygs6xqgldrbd4nfgzjc0000gn/T/spark-8335a0bc-0c43-44d7-b97c-2d39f96ac073
Finished...

Process finished with exit code 0

 */