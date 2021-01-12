package cloud.localstack.awssdkv1;

import cloud.localstack.LocalstackTestRunner;
import cloud.localstack.awssdkv1.TestUtils;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.internal.SdkInternalList;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.lang.System.out;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;

import cloud.localstack.Constants;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.fasterxml.jackson.*;

@RunWith(LocalstackTestRunner.class)
public class KinesisConsumerTest {

    @Test
    public void testGetRecord() throws Exception{
      // AmazonKinesisAsync kinesisClient = TestUtils.getClientKinesisAsync();
      AWSCredentials TEST_CREDENTIALS = new BasicAWSCredentials(
        Constants.TEST_ACCESS_KEY, Constants.TEST_SECRET_KEY);

      AmazonKinesisAsync kinesisClient = AmazonKinesisAsyncClientBuilder.standard().
                withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration( "http://localhost:4566",Constants.DEFAULT_REGION)).
                withCredentials(new AWSStaticCredentialsProvider(TEST_CREDENTIALS)).build();

      String streamName = "test-s-"+UUID.randomUUID().toString();

      CreateStreamRequest createStreamRequest = new CreateStreamRequest();
      createStreamRequest.setStreamName(streamName);
      createStreamRequest.setShardCount(1);

      kinesisClient.createStream(createStreamRequest);
      TimeUnit.SECONDS.sleep(2);

      PutRecordRequest putRecordRequest = new PutRecordRequest();
      putRecordRequest.setPartitionKey("partitionkey");
      putRecordRequest.setStreamName(streamName);
      putRecordRequest.setData(ByteBuffer.wrap("hello, world!".getBytes()));

      String shardId = kinesisClient.putRecord(putRecordRequest).getShardId();

      GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
      getShardIteratorRequest.setShardId(shardId);
      getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
      getShardIteratorRequest.setStreamName(streamName);

      String shardIterator = kinesisClient
        .getShardIterator(getShardIteratorRequest)
        .getShardIterator();

      GetRecordsRequest getRecordRequest = new GetRecordsRequest() ;
      getRecordRequest.setShardIterator(shardIterator);

      Integer limit = 100;
      Integer counter = 0;
      Boolean recordFound = false; 
      
      while (true) {
        getRecordRequest.setShardIterator(shardIterator);
        GetRecordsResult recordsResponse = kinesisClient.getRecords(getRecordRequest);
        
        List records = recordsResponse.getRecords();
        if (records.isEmpty()) {
          recordFound = true; 
          break;
        }

        if(counter >= limit){
          break;
        }

        counter += 1;
        shardIterator = recordsResponse.getNextShardIterator();
      }
      Assert.assertTrue(recordFound);
    }
}