package com.task11;

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
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.amazonaws.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

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

    public APIGatewayProxyResponseEvent handleReservationsGet(APIGatewayProxyRequestEvent requestEvent) {
        logger.log(">> handleReservationsGet");

        try {
            final List<ReservationRecord> reservationsList = getReservations();
            logger.log("reservations: " + reservationsList);

            ArrayList<Map<String, Object>> reservations = new ArrayList<>();
            for (ReservationRecord item : reservationsList) {
                Map<String, Object> itemValues = Map.of(
                        "tableNumber", item.tableNumber,
                        "clientName", item.clientName,
                        "phoneNumber", item.phoneNumber,
                        "date", item.date,
                        "slotTimeStart", item.slotTimeStart,
                        "slotTimeEnd", item.slotTimeEnd
                );
                reservations.add(itemValues);
            }

            Map<String, Object[]> response = Map.of(
                    "reservations", reservations.toArray()
            );

            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("ERROR: %s".formatted(Arrays.asList(e.getStackTrace())));
            logger.log("ERROR: Get items failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());
        }
    }

    public APIGatewayProxyResponseEvent handleReservationsPost(APIGatewayProxyRequestEvent requestEvent) {
        logger.log(">> handleReservationsPost");

        final ReservationRecord reservationRecord = gson.fromJson(requestEvent.getBody(), ReservationRecord.class);

        Table table = dynamoDB.getTable(reservationsDBTableName);
        try {
            final List<Integer> tableNumbers = getTableNumbers();
            logger.log("tableIds: " + tableNumbers);
            final List<ReservationRecord> reservations = getReservations();
            logger.log("reservations: " + reservations);

            if (!validatePostReservationsRequest(reservationRecord, tableNumbers, reservations)) {
                logger.log("ERROR: invalid reservation");
                return buildErrorResponse("Invalid reservation");
            }

            final String reservationId = UUID.randomUUID().toString();

            Item item = new Item()
                    .withPrimaryKey("id", reservationId)
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
            logger.log("ERROR: %s".formatted(Arrays.asList(e.getStackTrace())));
            logger.log("ERROR: Create items failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());

        }
    }

    private List<ReservationRecord> getReservations() {
        ScanRequest scanRequest = new ScanRequest().withTableName(reservationsDBTableName);
        ScanResult result = client.scan(scanRequest);

        return result.getItems().stream()
                .map(item ->
                        new ReservationRecord(
                                Integer.valueOf(item.get("tableNumber").getN()),
                                item.get("clientName").getS(),
                                item.get("phoneNumber").getS(),
                                item.get("date").getS(),
                                item.get("slotTimeStart").getS(),
                                item.get("slotTimeEnd").getS()))
                .collect(Collectors.toList());
    }

    private List<Integer> getTableNumbers() {
        ScanRequest scanRequest = new ScanRequest().withTableName(tablesDBTableName);
        ScanResult result = client.scan(scanRequest);

        return result.getItems().stream()
                .map(item -> Integer.valueOf(item.get("number").getN()))
                .collect(Collectors.toList());
    }

    public APIGatewayProxyResponseEvent handleTablesGet(APIGatewayProxyRequestEvent requestEvent) {
        logger.log(">> handleTablesGet");

        try {
            ScanRequest scanRequest = new ScanRequest().withTableName(tablesDBTableName);
            ScanResult result = client.scan(scanRequest);

            ArrayList<Map<String, Object>> tables = new ArrayList<>();
            for (Map<String, AttributeValue> item : result.getItems()) {
                logger.log("Table item: %s".formatted(item));

                Map<String, Object> itemValues = new HashMap<>();
                itemValues.put("id", Integer.valueOf(item.get("id").getS()));
                itemValues.put("number", Integer.valueOf(item.get("number").getN()));
                itemValues.put("places", Integer.valueOf(item.get("places").getN()));
                itemValues.put("isVip", item.get("isVip").getBOOL());

                if (item.get("minOrder") != null) {
                    itemValues.put("minOrder", Integer.valueOf(item.get("minOrder").getN()));
                }

                tables.add(itemValues);
            }

            Map<String, Object[]> response = Map.of(
                    "tables", tables.toArray()
            );

            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("ERROR: %s".formatted(Arrays.asList(e.getStackTrace())));
            logger.log("ERROR: Get item failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());
        }
    }

    public APIGatewayProxyResponseEvent handleTablesByIdGet(APIGatewayProxyRequestEvent requestEvent) {
        logger.log(">> handleTablesByIdGet");

        final String tableId = requestEvent.getPathParameters().get("tableId");
        Table table = dynamoDB.getTable(tablesDBTableName);
        try {
            Item item = table.getItem(new PrimaryKey("id", tableId));

            Map<String, Object> response = new HashMap<>();
            response.put("id", Integer.valueOf(item.getString("id")));
            response.put("number", item.getInt("number"));
            response.put("places", item.getInt("places"));
            response.put("isVip", item.getBoolean("isVip"));
            if (item.get("minOrder") != null) {
                response.put("minOrder", item.getInt("minOrder"));
            }

            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("ERROR: %s".formatted(Arrays.asList(e.getStackTrace())));
            logger.log("ERROR: Get item failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());
        }
    }

    public APIGatewayProxyResponseEvent handleTablesPost(APIGatewayProxyRequestEvent requestEvent) {
        logger.log(">> handleTablesPost");
        final TableRecord tableRecord = gson.fromJson(requestEvent.getBody(), TableRecord.class);

        Table table = dynamoDB.getTable(tablesDBTableName);
        try {
            final List<Integer> tableNumbers = getTableNumbers();
            logger.log("tableIds: " + tableNumbers);
            if (!validatePostTablesRequest(tableRecord)) {
                logger.log("ERROR: invalid table info");
                return buildErrorResponse("Invalid table info");
            }

            final String tableId = String.valueOf(tableRecord.id); //UUID.randomUUID().toString();

            Item item = new Item()
                    .withPrimaryKey("id", tableId)
                    .withNumber("number", tableRecord.number)
                    .withNumber("places", tableRecord.places)
                    .withBoolean("isVip", tableRecord.isVip);

            if (tableRecord.minOrder != null) {
                item.withNumber("minOrder", tableRecord.minOrder);
            }
            logger.log("Item: " + item);
            table.putItem(item);

            Map<String, Integer> response = Map.of("id", Integer.valueOf(tableId));
            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("ERROR: %s".formatted(Arrays.asList(e.getStackTrace())));
            logger.log("ERROR: Create items failed. %s".formatted(e.getMessage()), LogLevel.ERROR);
            return buildErrorResponse(e.getMessage());

        }
    }

    private boolean validatePostTablesRequest(TableRecord request) {
        if (request.id != null
                && request.number != null
                && request.places != null
                && request.isVip != null
        ) {
            return true;
        }

        return false;
    }

    private boolean validatePostReservationsRequest(ReservationRecord request, List<Integer> tableIds, List<ReservationRecord> reservations) {
        if (request.tableNumber != null
                && request.clientName != null
                && request.phoneNumber != null
                && request.date != null
                && request.slotTimeStart != null
                && request.slotTimeEnd != null
                && tableIds.contains(request.tableNumber)
                && noOverlap(request, reservations)
        ) {
            return true;
        }

        return false;
    }

    private boolean noOverlap(ReservationRecord reservation, List<ReservationRecord> reservations) {
        Integer tableNumber = reservation.tableNumber;
        String date = reservation.date;
        String startTime = reservation.slotTimeStart;
        String endTime = reservation.slotTimeEnd;

        for (ReservationRecord r : reservations) {
            if (Objects.equals(r.tableNumber, tableNumber) && Objects.equals(r.date, date)) {
                if (!(StringUtils.compare(r.slotTimeStart, endTime) >= 0
                        || StringUtils.compare(r.slotTimeEnd, startTime) <= 0)) {
                    return false;
                }
            }
        }


        return true;
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
    private record TableRecord(Integer id, Integer number, Integer places, Boolean isVip, Integer minOrder) {
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
