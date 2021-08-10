/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

/**
 * Implementation of the consumer that delivers the messages into Amazon Kinesis destination.
 *
 * @author Jiri Pechanec
 *
 */
@Named("kinesis")
@Dependent
public class KinesisChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.kinesis.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_ENDPOINT_NAME = PROP_PREFIX + "endpoint";

    private String region;
    private Optional<String> endpointOverride;
    private static FileWriter failfile;
    // private static File failfile;

    @ConfigProperty(name = PROP_PREFIX + "retry.enable", defaultValue = "false")
    String retryEnable;

    @ConfigProperty(name = PROP_PREFIX + "retry.count", defaultValue = "1")
    String retryCount;

    @ConfigProperty(name = PROP_PREFIX + "retry.delay.ms", defaultValue = "1000")
    String retryDelay;

    @ConfigProperty(name = PROP_PREFIX + "retry.fail.file.enable", defaultValue = "true")
    String retryfileEnable;

    @ConfigProperty(name = PROP_PREFIX + "retry.fail.file.location", defaultValue = "kinesis_")
    String retryfileLocation;

    @ConfigProperty(name = PROP_PREFIX + "credentials.profile", defaultValue = "default")
    String credentialsProfile;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    private KinesisClient client = null;

    @Inject
    @CustomConsumerBuilder
    Instance<KinesisClient> customClient;

    @PostConstruct
    void connect() {
        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Obtained custom configured KinesisClient '{}'", client);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        region = config.getValue(PROP_REGION_NAME, String.class);
        endpointOverride = config.getOptionalValue(PROP_ENDPOINT_NAME, String.class);
        final KinesisClientBuilder builder = KinesisClient.builder()
                .region(Region.of(region))
                .credentialsProvider(ProfileCredentialsProvider.create(credentialsProfile));
        endpointOverride.ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
        client = builder.build();
        LOGGER.info("Using default KinesisClient '{}'", client);
    }

    @PreDestroy
    void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Kinesis client: {}", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            final PutRecordRequest putRecord = PutRecordRequest.builder()
                    .partitionKey((record.key() != null) ? getString(record.key()) : nullKey)
                    .streamName(streamNameMapper.map(record.destination()))
                    .data(SdkBytes.fromByteArray(getBytes(record.value())))
                    .build();
            if (retryEnable.equals("false")) {
                client.putRecord(putRecord);
            }
            else {
                int rCount = Integer.parseInt(retryCount);
                int intDelay = Integer.parseInt(retryDelay);
                int putRes = 0;
                for (int i = 1; i <= rCount; i++) {
                    try {
                        client.putRecord(putRecord);
                        putRes++;
                        break;
                    }
                    catch (Exception e) {
                        LOGGER.warn("Exception while putting record to Kinesis: retry count:" + i);
                        LOGGER.warn("Exception:", e);
                        if (i < rCount) {
                            Thread.sleep(intDelay);
                        }
                    }
                }
                if (retryfileEnable.equals("true") && putRes == 0) {
                    try {
                        failfile = new FileWriter(retryfileLocation + UUID.randomUUID().toString() + ".json");
                        failfile.write(record.value().toString());
                    }
                    catch (Exception e) {
                        LOGGER.error("Exception while writing failed record to local file:", e);
                        throw new EmptyStackException();
                    }
                    finally {
                        try {
                            failfile.flush();
                            failfile.close();
                        }
                        catch (IOException ioe) {
                            LOGGER.error("Exception while closing local file:", ioe);
                            throw new EmptyStackException();
                        }
                    }

                }
            }
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }
}
