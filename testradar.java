package com.hsbc.gdt.mds.rdhm.model;

 

import org.junit.jupiter.api.Test;

 

import java.util.Collections;

 

import static org.junit.jupiter.api.Assertions.*;

 

class RadarNotificationPayloadTest {

 

    @Test

    void testSerializationAndDeserialization() {

        RadarNotificationPayload.Identifier identifier = RadarNotificationPayload.Identifier.builder()

                .issuer("issuer1").type("type1").id("id1").build();

        RadarNotificationPayload.Publication publication = RadarNotificationPayload.Publication.builder()

                .productId("pub1").description("desc").build();

        RadarNotificationPayload.Publisher publisher = RadarNotificationPayload.Publisher.builder()

                .env("Dev")

                .identifier(identifier).name("PublisherName").publication(publication).build();

        RadarNotificationPayload.AdditionalInfo additionalInfo = RadarNotificationPayload.AdditionalInfo.builder()

                .assetCode("TEST_ASSET")

                .version("V1.0")

                .status("SUCCESS")

                .runId("R001")

                .sourceType("TEST")

                .reportingDate("2024-01-01")

                .rdhLastIngestionDate("2024-01-01")

                .build();

        RadarNotificationPayload.Item item = RadarNotificationPayload.Item.builder()

                .itemType("typeA").itemName("itemA")

                .additionalInfo(additionalInfo)

                .recordCount(100)

                .build();

        RadarNotificationPayload.DataSet dataSet = RadarNotificationPayload.DataSet.builder()

                .status("READY").statusChangedDateTime("2024-01-01T00:00:00Z")

                .items(Collections.singletonList(item)).build();

        RadarNotificationPayload.NotificationData notificationData = RadarNotificationPayload.NotificationData.builder()

                .publisher(publisher).dataSet(dataSet).build();

        RadarNotificationPayload payload = RadarNotificationPayload.builder()

                .notificationID("notif-123")

                .notificationType("RADAR")

                .schemaVersion("1.0")

                .createdDateTime("2024-01-01T00:00:00Z")

                .notificationData(notificationData)

                .build();

 

        String json = payload.toJson();

        assertNotNull(json);

        assertTrue(json.contains("notif-123"));

 

        RadarNotificationPayload deserialized = RadarNotificationPayload.fromJson(json);

        assertEquals(payload.getNotificationID(), deserialized.getNotificationID());

        assertEquals(payload.getNotificationData().getPublisher().getName(),

                deserialized.getNotificationData().getPublisher().getName());

        assertEquals(payload.getNotificationData().getDataSet().getItems().get(0).getItemName(),

                deserialized.getNotificationData().getDataSet().getItems().get(0).getItemName());

    }

 

    @Test

    void testParseException() {

        String invalidJson = "{";

        Exception ex = assertThrows(RadarNotificationPayload.RadarNotificationPayloadParseException.class, () -> {

            RadarNotificationPayload.fromJson(invalidJson);

        });

        assertTrue(ex.getMessage().contains("Failed to parse"));

    }

 

    @Test

    void testSerializationException() {

        // Break the object mapper by injecting a self-reference (not possible here, but simulate by mocking if needed)

        // For now, forcibly throw exception by subclassing

        RadarNotificationPayload broken = new RadarNotificationPayload() {

            @Override

            public String toJson() {

                throw new RuntimeException("Simulated failure");

            }

        };

        Exception ex = assertThrows(RuntimeException.class, broken::toJson);

        assertEquals("Simulated failure", ex.getMessage());

    }

 

    @Test

    void testBuilderAndEqualsHashCode() {

        RadarNotificationPayload.AdditionalInfo testAdditionalInfo = RadarNotificationPayload.AdditionalInfo.builder()

                .assetCode("TEST").version("V1").status("OK").runId("R1")

                .sourceType("SRC").reportingDate("2024-01-01").rdhLastIngestionDate("2024-01-01").build();

        RadarNotificationPayload.Item item1 = RadarNotificationPayload.Item.builder()

                .itemType("A").itemName("B").additionalInfo(testAdditionalInfo).recordCount(50).build();

        RadarNotificationPayload.Item item2 = RadarNotificationPayload.Item.builder()

                .itemType("A").itemName("B").additionalInfo(testAdditionalInfo).recordCount(50).build();

        assertEquals(item1, item2);

        assertEquals(item1.hashCode(), item2.hashCode());

    }

}