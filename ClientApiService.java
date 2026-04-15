package com.hsbc.gdt.mds.rdhm.service;

 

import com.fasterxml.jackson.databind.JsonNode;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hsbc.gdt.mds.rdhm.config.ClientConfig;

import com.hsbc.gdt.mds.rdhm.config.WebhookServiceCredentialsProperties;

import com.hsbc.gdt.mds.rdhm.model.NotificationPayload;

import com.hsbc.gdt.mds.rdhm.model.RadarNotificationPayload;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.http.*;

import org.springframework.http.client.ClientHttpResponse;

import org.springframework.stereotype.Service;

import org.springframework.web.client.ResponseErrorHandler;

import org.springframework.web.client.RestTemplate;

 

import java.time.OffsetDateTime;

import java.time.ZoneOffset;

import java.time.format.DateTimeFormatter;

import java.util.List;

import java.util.UUID;

 

@Service

public class ClientApiService {

 

    private static final Logger log = LoggerFactory.getLogger(ClientApiService.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

 

    private static final String DATASET_STATUS_READY = "Ready";

    private static final String HEADER_COUNTRY_CODE = "HK";

    private static final String HEADER_CLIENT_ID = "RDH";

    private static final String HEADER_SOURCE_SYSTEM_ID = "9594666";

    private static final String ITEM_TYPE = "Physical_Asset";

    private static final String ENV = "Dev";

 

    private final WebhookServiceCredentialsProperties webhookServiceCredentialsProperties;

    private final RestTemplate restTemplate;

 

    @Value("${com.hsbc.ce.gdt.refdata.eimId}")

    private String eimId;

 

    @Autowired

    public ClientApiService(

            RestTemplate restTemplate,

            WebhookServiceCredentialsProperties webhookServiceCredentialsProperties) {

        this.restTemplate = restTemplate;

        this.webhookServiceCredentialsProperties = webhookServiceCredentialsProperties;

    }

 

    public String generateClientJson(NotificationPayload notificationPayload, ClientConfig clientConfig) {

        String originalUpdateTime = notificationPayload.getData().getUpdateTime();

        OffsetDateTime odt = parseDateTime(originalUpdateTime);

 

        String formattedCreatedDateTime = odt.withOffsetSameInstant(ZoneOffset.UTC)

                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX"));

 

        String formattedStatusChangedDateTime = odt.withOffsetSameInstant(ZoneOffset.UTC)

                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX"));

 

        RadarNotificationPayload req = RadarNotificationPayload.builder()

                .notificationID(UUID.randomUUID().toString())

                .notificationType("DataReadiness")

                .schemaVersion("https://radar.hsbc/schemas/notifications/datareadiness/v2.0")

                .createdDateTime(formattedCreatedDateTime)

                .notificationData(

                        RadarNotificationPayload.NotificationData.builder()

                                .publisher(

                                        RadarNotificationPayload.Publisher.builder()

                                                .env(ENV)

                                                .identifier(

                                                        RadarNotificationPayload.Identifier.builder()

                                                                .issuer("EIM")

                                                                .type("appInstId")

                                                                .id(eimId)

                                                                .build()

                                                )

                                                .name(clientConfig.getPublisherName())

                                                .publication(

                                                        RadarNotificationPayload.Publication.builder()

                                                                .productId(clientConfig.getPublicationId())

                                                                .description(clientConfig.getPublicationId())

                                                                .build()

                                                )

                                                .build()

                                )

                                .dataSet(

                                        RadarNotificationPayload.DataSet.builder()

                                                .status(DATASET_STATUS_READY)

                                                .statusChangedDateTime(formattedStatusChangedDateTime)

                                                .items(List.of(

                                                        RadarNotificationPayload.Item.builder()

                                                                .itemType(ITEM_TYPE)

                                                                .itemName(notificationPayload.getData().getAssetType())

                                                                .additionalInfo(
                                                                        RadarNotificationPayload.AdditionalInfo.builder()
                                                                                .assetCode(notificationPayload.getData().getAssetCode())
                                                                                .version(notificationPayload.getData().getVersion())
                                                                                .status(notificationPayload.getData().getStatus())
                                                                                .runId(notificationPayload.getData().getRunId())
                                                                                .sourceType(notificationPayload.getData().getSourceType())
                                                                                .reportingDate(notificationPayload.getData().getReportingDate())
                                                                                .rdhLastIngestionDate(notificationPayload.getData().getRdhLastIngestionDate())
                                                                                .build()
                                                                )

                                                                .recordCount(notificationPayload.getData().getRecordCount())

                                                                .build()

                                                ))

                                                .build()

                                )

                                .build()

                )

                .build();

        String json = req.toJson();

        log.debug("Generated client JSON: {}", json);

        return json;

    }

 

    public boolean callClientEndpoint(ClientConfig clientConfig, String token, String json) {

        HttpHeaders headers = getHttpHeaders(token);

        HttpEntity<String> entity = new HttpEntity<>(json, headers);

 

        String notificationId = extractNotificationId(json);

 

        restTemplate.setErrorHandler(new ResponseErrorHandler() {

            @Override

            public boolean hasError(ClientHttpResponse response) { return false; }

            @Override

            public void handleError(ClientHttpResponse response) {

                // This method is intentionally left empty because hasError always returns false,

                // meaning Spring will never call handleError. If you want to handle errors,

                // either implement logic here or throw an UnsupportedOperationException.

                // throw new UnsupportedOperationException("Error handling not implemented");

            }

        });

 

        try {

            log.info("Calling client endpoint: endpoint={}, notificationID={}, correlationId={}, sessionId={}",

                    clientConfig.getEndpoint(),

                    notificationId,

                    headers.getFirst("X-HSBC-Request-Correlation-Id"),

                    headers.getFirst("X-HSBC-Session-Correlation-Id"));

            log.debug("Request payload for notificationID={}: {}", notificationId, json);

            log.debug("Request headers: {}", headers);

 

            ResponseEntity<String> response = restTemplate.postForEntity(clientConfig.getEndpoint(), entity, String.class);

            log.info("Client endpoint response status: {}, notificationID={}, endpoint={}",

                    response.getStatusCode(), notificationId, clientConfig.getEndpoint());

            log.debug("Client endpoint response for notificationID={}: {}", notificationId, response.getBody());

            if (response.getStatusCode() == HttpStatus.OK) {

                log.debug("Client endpoint call successful for notificationID={}. Response: {}", notificationId, response.getBody());

                return true;

            } else {

                log.warn("Client endpoint call failed for notificationID={}. Status: {}, Body: {}, endpoint={}",

                        notificationId, response.getStatusCode(), response.getBody(), clientConfig.getEndpoint());

                return false;

            }

        } catch (Exception e) {

            log.error("Error calling client endpoint: endpoint={}, notificationID={}, error={}",

                    clientConfig.getEndpoint(), notificationId, e.getMessage(), e);

            return false;

        }

    }

 

    HttpHeaders getHttpHeaders(String token) {

        HttpHeaders headers = new HttpHeaders();

        headers.setContentType(MediaType.APPLICATION_JSON);

        headers.set("X-HSBC-Chnl-CountryCode", HEADER_COUNTRY_CODE);

        headers.set("X-HSBC-Client-Id", HEADER_CLIENT_ID);

        headers.set("X-HSBC-E2E-Trust-Token", token);

        headers.set("X-HSBC-Request-Correlation-Id", UUID.randomUUID().toString());

        headers.set("X-HSBC-Session-Correlation-Id", UUID.randomUUID().toString());

        headers.set("X-HSBC-Source-System-Id", HEADER_SOURCE_SYSTEM_ID);

        headers.set("X-HSBC-User-Id", webhookServiceCredentialsProperties.getUsername());

        return headers;

    }

 

    private static OffsetDateTime parseDateTime(String dateTimeStr) {

        // Handles input like "2025-12-30 09:28:13.156000+00:00"

        return OffsetDateTime.parse(

                dateTimeStr.replace(" ", "T").replaceAll("(\\.\\d{3})\\d*([+-])", "$1$2")

        );

    }

 

    private static String extractNotificationId(String json) {

        try {

            JsonNode node = OBJECT_MAPPER.readTree(json);

            return node.has("notificationID") ? node.get("notificationID").asText() : null;

        } catch (Exception ex) {

            // Only warn if notificationID is missing, not a hard failure

            LoggerFactory.getLogger(ClientApiService.class)

                    .warn("Unable to extract notificationID from payload JSON", ex);

            return null;

        }

    }

}

 