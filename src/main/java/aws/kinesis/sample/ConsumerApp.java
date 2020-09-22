package aws.kinesis.sample;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.log4j.BasicConfigurator;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.kinesis.KinesisInputDStream;


public class ConsumerApp
{
    public static void main( String[] args ) throws JsonProcessingException {
       // ConsumerApp object = new ConsumerApp();
       /* KinesisInputDStream<Builder> kinesisStream = KinesisInputDStream.builder()
                .streamingContext(new StreamingContext("test"))
                //.endpointUrl("https://s3.console.aws.amazon.com/s3/buckets/test-stream-spark")
                .regionName("us-east-2")
                .streamName("test-stream")
                //.initialPosition(KinesisInitialPositions.Latest())
                .checkpointAppName("test-app")
                //.checkpointInterval(Duration.apply(2000))
                .storageLevel(StorageLevel.MEMORY_ONLY());
                //.metricsLevel(MetricsLevel.DETAILED)
                //.metricsEnabledDimensions(JavaConverters.asScalaSetConverter(KinesisClientLibConfiguration.DEFAULT_METRICS_ENABLED_DIMENSIONS).asScala().toSet())
                //.build();*/
        //https://kinesis.us-east-2.amazonaws.com
        BasicConfigurator.configure();
                KinesisInputDStream<byte[]> kinesisStream = KinesisInputDStream.builder()
                .streamingContext(new StreamingContext("test"))
             .regionName("us-east-2")
             .streamName("test-stream")
              .endpointUrl("https://kinesis.us-east-2.amazonaws.com")
            // .initialPosition(KinesisInitialPositions.Latest())
             .checkpointAppName("test-app")
             .checkpointInterval(Duration.apply(2000))
             .storageLevel(StorageLevel.MEMORY_ONLY())
                //.metricsLevel(MetricsLevel.DETAILED)
                //.metricsEnabledDimensions(JavaConverters.asScalaSetConverter(KinesisClientLibConfiguration.DEFAULT_METRICS_ENABLED_DIMENSIONS).asScala().toSet())
                //.buildWithMessageHandler([message handler]);
             .build();
    }

   /* *//*def prepareStream(ssc: StreamingContext, region: Region, shardNum: Int): DStream[Array[Byte]] = {
    // Create same number of consumer streams as there is shards
    println("Config.getAppName: " + Config.getAppName)
    val streams = (0 until shardNum).map { _ =>
        val builder = KinesisInputDStream.builder
                .streamingContext(ssc)
                .checkpointAppName(Config.getAppName)
                .streamName(Config.getStreamName)
                .regionName(region.getName)
                .checkpointInterval(Seconds(30))
                .storageLevel(StorageLevel.MEMORY_ONLY)

        Config.getCredentials.foreach(builder.kinesisCredentials)

        builder.buildWithMessageHandler(record => { // map records to ByteArray
                val byteBuffer = record.getData
                val byteArray = new Array[Byte](byteBuffer.remaining())
                byteBuffer.get(byteArray)
                byteArray*//*
        })
    }
    ssc.union(streams) // all streams will be handled equally*/
}


