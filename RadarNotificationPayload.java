package com.hsbc.gdt.mds.rdhm.model;

 

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.*;

 

import java.util.List;

 

@Data

@Builder

@AllArgsConstructor

@NoArgsConstructor

public class RadarNotificationPayload {

 

    private static final ObjectMapper objectMapper = new ObjectMapper();

 

    private String notificationID;

    private String notificationType;

    private String schemaVersion;

    private String createdDateTime;

    private NotificationData notificationData;

 

    @Data @Builder @AllArgsConstructor @NoArgsConstructor

    public static class NotificationData {

        private Publisher publisher;

        private DataSet dataSet;

    }

 

    @Data @Builder @AllArgsConstructor @NoArgsConstructor

    public static class Publisher {

        private String env;

        private Identifier identifier;

        private String name;

        private Publication publication;

    }

 

    @Data @Builder @AllArgsConstructor @NoArgsConstructor

    public static class Publication {

        private String productId;

        private String description;

    }

 

    @Data   @Builder @AllArgsConstructor @NoArgsConstructor

    public static class DataSet {

        private String status;

        private String statusChangedDateTime;

        private List<Item> items;

    }

 

    @Data @Builder @AllArgsConstructor @NoArgsConstructor

    public static class AdditionalInfo {

        private String assetCode;

        private String version;

        private String runId;

        private String sourceType;

        private String reportingDate;

        private String rdhLastIngestionDate;

        private String status;

    }

 

    @Data @Builder @AllArgsConstructor @NoArgsConstructor

    public static class Item {

        private String itemType;

        private String itemName;

        private AdditionalInfo additionalInfo;

        private int recordCount;

    }

 

    @Data @Builder @AllArgsConstructor @NoArgsConstructor

    public static class Identifier {

        private String issuer;

        private String type;

        private String id;

    }

 

    public static RadarNotificationPayload fromJson(String json) {

        try {

            return objectMapper.readValue(json, RadarNotificationPayload.class);

        } catch (Exception e) {

            throw new RadarNotificationPayloadParseException("Failed to parse RadarNotificationPayload JSON", e);

        }

    }

 

    public String toJson() {

        try {

            return objectMapper.writeValueAsString(this);

        } catch (Exception e) {

            throw new RadarNotificationPayloadSerializationException("Failed to serialize RadarNotificationPayload to JSON", e);

        }

    }

 

    // Dedicated exception for parse errors

    public static class RadarNotificationPayloadParseException extends RuntimeException {

        public RadarNotificationPayloadParseException(String message, Throwable cause) {

            super(message, cause);

        }

    }

 

    // Dedicated exception for serialization errors

    public static class RadarNotificationPayloadSerializationException extends RuntimeException {

        public RadarNotificationPayloadSerializationException(String message, Throwable cause) {

            super(message, cause);

        }

    }

}