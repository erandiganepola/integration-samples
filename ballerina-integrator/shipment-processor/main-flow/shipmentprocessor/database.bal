//import ballerina/lang.runtime;
import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerinax/mysql;

// Insert enriched shipment record to MySQL database
public function insertEnrichedShipmentToDb(EnrichedShipment enrichedShipment) returns DatabaseResult {
    if !enableDatabaseStorage {
        return {success: false, recordsInserted: 0, errorMessage: "Database storage is disabled"};
    }

    mysql:Client|error dbClient = getMysqlClient();
    if dbClient is error {
        log:printError("Failed to get MySQL client", 'error = dbClient);
        return {success: false, recordsInserted: 0, errorMessage: dbClient.message()};
    }

    ShipmentProduct shipmentProduct = mapEnrichedShipmentToDbRecord(enrichedShipment);

    sql:ParameterizedQuery insertQuery = `
        INSERT INTO shipment_products (
            shipment_id, shipment_date, csv_product_code, customer_email, correlation_id, order_id, customer_id, customer_name,
            origin, destination, status, created_date, estimated_delivery_date,
            actual_delivery_date, total_weight, carrier, tracking_number, processed_at, products_json
        ) VALUES (
            ${shipmentProduct.shipment_id}, ${shipmentProduct.shipment_date}, 
            ${shipmentProduct.csv_product_code}, ${shipmentProduct.customer_email}, ${shipmentProduct.correlation_id}, 
            ${shipmentProduct.order_id}, ${shipmentProduct.customer_id}, ${shipmentProduct.customer_name}, 
            ${shipmentProduct.origin}, ${shipmentProduct.destination}, ${shipmentProduct.status}, 
            ${shipmentProduct.created_date}, ${shipmentProduct.estimated_delivery_date}, 
            ${shipmentProduct.actual_delivery_date}, ${shipmentProduct.total_weight}, 
            ${shipmentProduct.carrier}, ${shipmentProduct.tracking_number}, 
            ${shipmentProduct.processed_at}, ${shipmentProduct.products_json}
        )
        ON DUPLICATE KEY UPDATE
            shipment_date = VALUES(shipment_date),
            csv_product_code = VALUES(csv_product_code),
            customer_email = VALUES(customer_email),
            correlation_id = VALUES(correlation_id),
            products_json = VALUES(products_json),
            processed_at = VALUES(processed_at)
    `;

    sql:ExecutionResult|sql:Error result = dbClient->execute(insertQuery);
    if result is sql:Error {
        log:printError("Failed to insert shipment: " + enrichedShipment.shipmentId, 'error = result);
        return {success: false, recordsInserted: 0, errorMessage: result.message()};
    }

    return {success: true, recordsInserted: result.affectedRowCount ?: 0, errorMessage: ()};
}

// Insert multiple enriched shipments to database asynchronously 
public function insertAndNotifyEnrichedShipmentsAsync(EnrichedShipment[] enrichedShipments, string correlationId) returns DatabaseResult {
    if !enableDatabaseStorage {
        return {success: false, recordsInserted: 0, errorMessage: "Database storage is disabled"};
    }

    if enrichedShipments.length() == 0 {
        return {success: true, recordsInserted: 0, errorMessage: ()};
    }

    log:printInfo("Starting async batch insert of " + enrichedShipments.length().toString() + " enriched shipments");
    
    // Process in smaller batches to manage memory for large datasets
    int totalRecords = enrichedShipments.length();
    int totalInserted = 0;
    string[] errors = [];

    int processedCount = 0;
    while processedCount < totalRecords {
        int endIndex = int:min(processedCount + maxDatabaseBatchSize, totalRecords);
        EnrichedShipment[] batchChunk = enrichedShipments.slice(processedCount, endIndex);

        // Process this chunk 
        foreach EnrichedShipment enrichedShipment in batchChunk {
            future<DatabaseResult> _ = start insertEnrichedShipmentToDb(enrichedShipment);
            // For demo purposes, limit Kafka event publishing to first N shipments to prevent email overload
            if (sentEmailCount < kafkaEventPublishCount) {
                publishShipmentEvent(enrichedShipment, correlationId);
                sentEmailCount += 1;
            }
            totalInserted += 1;
        }

        processedCount = endIndex;

        // Clear the processed chunk to free memory
        batchChunk = [];

        // Log progress and brief pause for memory management
        if totalRecords > 100 {
            log:printInfo(string `Database batch progress: ${processedCount}/${totalRecords} records processed`);
        }

    }

    boolean success = errors.length() == 0;
    string? errorMessage = errors.length() > 0 ? string:'join(", ", ...errors) : ();

    log:printInfo("Batch insert completed: " + totalInserted.toString() + " records inserted, " +
                errors.length().toString() + " errors");

    return {success, recordsInserted: totalInserted, errorMessage};
}

// Helper function to convert products to JSON string
function productsToJsonString(Product[] products) returns string {
    return products.length() > 0 ? products.toBalString() : "[]";
}

// Map enriched shipment to database record
function mapEnrichedShipmentToDbRecord(EnrichedShipment enrichedShipment) returns ShipmentProduct {
    return {
        shipment_id: enrichedShipment.shipmentId,
        shipment_date: enrichedShipment.shipmentDate,
        csv_product_code: enrichedShipment.csvProductCode,
        customer_email: enrichedShipment.customerEmail,
        correlation_id: enrichedShipment.correlationId,
        order_id: enrichedShipment.orderId,
        customer_id: enrichedShipment.customerId,
        customer_name: enrichedShipment.customerName,
        origin: enrichedShipment.origin,
        destination: enrichedShipment.destination,
        status: enrichedShipment.status.toString(),
        created_date: enrichedShipment.createdDate,
        estimated_delivery_date: enrichedShipment.estimatedDeliveryDate,
        actual_delivery_date: enrichedShipment.actualDeliveryDate,
        total_weight: enrichedShipment.totalWeight,
        carrier: enrichedShipment.carrier,
        tracking_number: enrichedShipment.trackingNumber,
        processed_at: time:utcToString(time:utcNow()),
        products_json: productsToJsonString(enrichedShipment.products)
    };
}
