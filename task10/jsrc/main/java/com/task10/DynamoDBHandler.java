package com.task10;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

public class DynamoDBHandler extends AbstractRequestHandlers {

    private LambdaLogger logger;
    private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    private static DynamoDB dynamoDB = new DynamoDB(client);
    private String tablesDBTableName;
    private String reservationsDBTableName;

    public DynamoDBHandler(Context context) {
        logger = context.getLogger();
        tablesDBTableName = System.getenv("tables_table");
        reservationsDBTableName = System.getenv("reservations_table");
    }

    public APIGatewayV2HTTPResponse handleReservationsGet(APIGatewayV2HTTPEvent requestEvent) {
        logger.log(">> handleReservationsGet");

        Table table = dynamoDB.getTable(tablesDBTableName);
        try {
            ScanRequest scanRequest = new ScanRequest().withTableName(reservationsDBTableName);
            ScanResult result = client.scan(scanRequest);

            ArrayList<Map<String, Object>> reservations = new ArrayList<>();
            for (Map<String, AttributeValue> item : result.getItems()) {
                Map<String, Object> itemValues = Map.of(
                        "tableNumber", item.get("tableNumber").getN(),
                        "clientName", item.get("clientName").getS(),
                        "phoneNumber", item.get("phoneNumber").getS(),
                        "date", item.get("date").getS(),
                        "slotTimeStart", item.get("slotTimeStart").getS(),
                        "slotTimeEnd", item.get("slotTimeEnd").getS()
                );
                reservations.add(itemValues);
            }

            Map<String, Object[]> response = Map.of(
                    "reservations", reservations.toArray()
            );

            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("Get items failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());
        }
    }

    public APIGatewayV2HTTPResponse handleReservationsPost(APIGatewayV2HTTPEvent requestEvent) {
        logger.log(">> handleReservationsPost");

        final ReservationRecord reservationRecord = gson.fromJson(requestEvent.getBody(), ReservationRecord.class);

        Table table = dynamoDB.getTable(reservationsDBTableName);
        try {
            final String reservationId = UUID.randomUUID().toString();

            Item item = new Item()
                    .withPrimaryKey("reservationId", reservationId)
                    .withNumber("tableNumber", reservationRecord.tableNumber)
                    .withString("clientName", reservationRecord.clientName)
                    .withString("phoneNumber", reservationRecord.phoneNumber)
                    .withString("date", reservationRecord.date)
                    .withString("slotTimeStart", reservationRecord.slotTimeStart)
                    .withString("slotTimeEnd", reservationRecord.slotTimeEnd);

            logger.log("Item: " + item);
            table.putItem(item);

            Map<String, String> response = Map.of("reservationId", reservationId);
            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("Create items failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());

        }
    }

    public APIGatewayV2HTTPResponse handleTablesGet(APIGatewayV2HTTPEvent requestEvent) {
        logger.log(">> handleTablesGet");

        try {
            ScanRequest scanRequest = new ScanRequest().withTableName(tablesDBTableName);
            ScanResult result = client.scan(scanRequest);

            ArrayList<Map<String, Object>> tables = new ArrayList<>();
            for (Map<String, AttributeValue> item : result.getItems()) {
                Map<String, Object> itemValues = Map.of(
                        "id", item.get("id").getN(),
                        "number", item.get("number").getN(),
                        "places", item.get("places").getN(),
                        "isVip", item.get("isVip").getB()
                );
                if (item.get("minOrder") != null) {
                    itemValues.put("minOrder", item.get("minOrder").getN());
                }

                tables.add(itemValues);
            }

            Map<String, Object[]> response = Map.of(
                    "tables", tables.toArray()
            );

            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("Get item failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());
        }
    }

    public APIGatewayV2HTTPResponse handleTablesByIdGet(APIGatewayV2HTTPEvent requestEvent) {
        logger.log(">> handleTablesByIdGet");

        final String tableId = requestEvent.getPathParameters().get("tableId");
        Table table = dynamoDB.getTable(tablesDBTableName);
        try {
            Item item = table.getItem(new PrimaryKey("id", tableId));

            Map<String, Object> response = Map.of(
                    "id", item.getInt("id"),
                    "number", item.getInt("number"),
                    "places", item.getInt("places"),
                    "isVip", item.getBoolean("isVip")
            );
            if (item.get("minOrder") != null) {
                response.put("minOrder", item.getInt("minOrder"));
            }

            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("Get item failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());
        }
    }

    public APIGatewayV2HTTPResponse handleTablesPost(APIGatewayV2HTTPEvent requestEvent) {
        logger.log(">> handleTablesPost");
        final TableRecord tableRecord = gson.fromJson(requestEvent.getBody(), TableRecord.class);

        Table table = dynamoDB.getTable(tablesDBTableName);
        try {
            final Integer id = tableRecord.id; //UUID.randomUUID().toString();

            Item item = new Item()
                    .withPrimaryKey("id", tableRecord.id)
                    .withNumber("number", tableRecord.number)
                    .withNumber("places", tableRecord.places)
                    .withBoolean("isVip", tableRecord.isVip);

            if (tableRecord.minOrder != null) {
                item.withNumber("minOrder", tableRecord.minOrder);
            }
            logger.log("Item: " + item);
            table.putItem(item);

            Map<String, Integer> response = Map.of("id", id);
            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("Create items failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());

        }
    }

    /**
     * {
     * "id": // int
     * "number": // int, number of the table
     * "places": // int, amount of people to sit at the table
     * "isVip": // boolean, is the table in the VIP hall
     * "minOrder": // optional. int, table deposit required to book it
     * }
     */
    private record TableRecord(Integer id, Integer number, Integer places, boolean isVip, Integer minOrder) {
    }

    /**
     * {
     * "tableNumber": // int, number of the table
     * "clientName": //string
     * "phoneNumber": //string
     * "date": // string in yyyy-MM-dd format
     * "slotTimeStart": // string in "HH:MM" format, like "13:00",
     * "slotTimeEnd": // string in "HH:MM" format, like "15:00"
     * }
     */
    private record ReservationRecord(Integer tableNumber,
                                     String clientName,
                                     String phoneNumber,
                                     String date,
                                     String slotTimeStart,
                                     String slotTimeEnd) {
    }
}
