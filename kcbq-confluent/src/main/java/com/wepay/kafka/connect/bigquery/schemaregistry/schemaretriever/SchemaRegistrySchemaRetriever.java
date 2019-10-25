package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema.Parser;

import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.Map;
import java.net.URL;
import java.net.MalformedURLException;

/**
 * Uses the Confluent Schema Registry to fetch the latest schema for a given topic.
 */
public class SchemaRegistrySchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(SchemaRegistrySchemaRetriever.class);

  private SchemaRegistryClient schemaRegistryClient;
  private AvroData avroData;

  /**
   * Only here because the package-private constructor (which is only used in testing) would
   * otherwise cover up the no-args constructor.
   */
  public SchemaRegistrySchemaRetriever() {
  }

  // For testing purposes only
  SchemaRegistrySchemaRetriever(SchemaRegistryClient schemaRegistryClient, AvroData avroData) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.avroData = avroData;
  }

  @Override
  public void configure(Map<String, String> properties) {
		SchemaRegistrySchemaRetrieverConfig config = new SchemaRegistrySchemaRetrieverConfig(properties);
		URL url;
		try 
		{
			logger.info("Building SchemaRegistry authentication support from URL.");
			url = new URL(config.getString(config.LOCATION_CONFIG));
			String userInfo = url.getUserInfo();
			String[] userInfoArray = userInfo.split(":");
			String auth = "Basic " + java.util.Base64.getEncoder().encodeToString((userInfoArray[0] + ":" + userInfoArray[1]).getBytes(java.nio.charset.StandardCharsets.UTF_8));
			io.confluent.kafka.schemaregistry.client.rest.RestService.DEFAULT_REQUEST_PROPERTIES.put("Authorization", auth);
			logger.info("Added auth to schema registry client");
		} 
		catch (MalformedURLException e) 
		{
			logger.error("Error connecting to the Schema registry: ", e);
			//e.printStackTrace();
		}
		schemaRegistryClient =
				new CachedSchemaRegistryClient(config.getString(config.LOCATION_CONFIG), 0);
		avroData = new AvroData(config.getInt(config.AVRO_DATA_CACHE_SIZE_CONFIG));
  }

  @Override
  public Schema retrieveSchema(TableId table, String topic) {
    try {
      String subject = getSubject(topic);
      logger.debug("Retrieving schema information for topic {} with subject {}", topic, subject);
      SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
      org.apache.avro.Schema avroSchema = new Parser().parse(latestSchemaMetadata.getSchema());
      return avroData.toConnectSchema(avroSchema);
    } catch (IOException | RestClientException exception) {
      throw new ConnectException(
          "Exception encountered while trying to fetch latest schema metadata from Schema Registry",
          exception
      );
    }
  }

  @Override
  public void setLastSeenSchema(TableId table, String topic, Schema schema) { }

  private String getSubject(String topic) {
    return topic + "-value";
  }
}
