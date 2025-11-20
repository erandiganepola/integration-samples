import ballerina/ftp;
import ballerina/http;
import ballerina/io;
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerina/file;

// Global map to track processed files to avoid re-downloading
map<boolean> processedFiles = {};

// Process a single file with download-and-process approach
function processFile(ftp:Caller caller, ftp:FileInfo fileInfo, string correlationId) returns error? {
    string fileName = fileInfo.name;

    // Check if file has already been processed in this session (if optimization is enabled)
    if enableFileTrackingOptimization && processedFiles.hasKey(fileName) {
        log:printInfo(string `Skipping already processed file: ${fileName}`, correlationId = correlationId);
        return;
    }

    do {
        // Step 1: Download file to temporary location
        string tempFilePath = check downloadFileFromFTP(caller, fileInfo, correlationId);

        // Step 2: Process the downloaded CSV file and get results
        ProcessingResult result = check processDownloadedCsvFile(tempFilePath, fileName, correlationId);

        // Step 3: Log results and rename original file
        logProcessingResult(result, fileName, correlationId);
        check renameProcessedFile(fileName, correlationId);

        // Step 4: Clean up temporary file
        check cleanupTempFile(tempFilePath, correlationId);

        // Mark file as processed to avoid re-processing (if optimization is enabled)
        if enableFileTrackingOptimization {
            processedFiles[fileName] = true;
        }

    } on fail error e {
        log:printError(string `Error processing file ${fileName}: ${e.message()}`,
                correlationId = correlationId);
        return e;
    }
}

// Download file from FTP to temporary location
function downloadFileFromFTP(ftp:Caller caller, ftp:FileInfo fileInfo, string correlationId) returns string|error {
    string tempDir = "/tmp/file_processing";
    string procFilePath = tempDir + "/" + fileInfo.name + "_proc";

    // Check if file already exists in temp directory (if optimization is enabled)
    if enableFileTrackingOptimization {
        byte[]|io:Error fileCheckResult = io:fileReadBytes(procFilePath);
        if fileCheckResult is byte[] {
            log:printInfo(string `File already exists in temp directory: ${procFilePath}`, correlationId = correlationId);
            return procFilePath;
        }
    }

    log:printInfo(string `Downloading file ${fileInfo.name} to ${procFilePath}`, correlationId = correlationId);

    // Download file stream from FTP
    stream<byte[] & readonly, io:Error?> fileStream = check caller->get(fileInfo.pathDecoded);

    // Write stream directly to file with _proc suffix
    check io:fileWriteBlocksFromStream(procFilePath, fileStream);
    check fileStream.close();

    log:printInfo(string `Successfully downloaded and saved ${fileInfo.name} as ${procFilePath}`, correlationId = correlationId);
    return procFilePath;
}

// Process downloaded CSV file 
function processDownloadedCsvFile(string filePath, string fileName, string correlationId) returns ProcessingResult|error {
    log:printInfo("Processing CSV file:", fileName = fileName, filePath = filePath, correlationId = correlationId);

    ProcessingResult result = initializeProcessingResult();
    string[][] currentBatch = [];
    int batchNumber = 1;
    boolean headerSkipped = false;
    int totalBatchesProcessed = 0;

    // Read CSV file as stream 
    stream<string[], io:Error?> csvStream = check io:fileReadCsvAsStream(filePath);

    do {
        // Process each CSV row from the stream
        while true {
            record {|string[] value;|}|io:Error? row = csvStream.next();

            if row is () {
                log:printInfo("CSV file processing completed - no more data rows", correlationId = correlationId);
                break;
            }

            if row is io:Error {
                return error(string `Error reading CSV row: ${row.message()}`);
            }

            string[] csvRow = row.value;

            // Skip header row
            if !headerSkipped && csvRow.length() > 0 && csvRow[0].toLowerAscii() == "shipmentid" {
                headerSkipped = true;
                continue;
            }

            // Skip empty rows
            if csvRow.length() == 0 || (csvRow.length() == 1 && csvRow[0].trim().length() == 0) {
                continue;
            }

            currentBatch.push(csvRow);

            // Process batch when it reaches the configured size
            if currentBatch.length() >= batchSize {
                processBatchAndAggregateResults(currentBatch, fileName, batchNumber, correlationId, result);

                totalBatchesProcessed += 1;

                currentBatch = [];
                batchNumber += 1;
            }
        }

        // Process any remaining records in the final batch
        if currentBatch.length() > 0 {
            processBatchAndAggregateResults(currentBatch, fileName, batchNumber, correlationId, result);
            totalBatchesProcessed += 1;
        }

        check csvStream.close();

        log:printInfo(string `CSV processing completed: ${fileName}, batches: ${batchNumber}, total: ${result.totalRecords}`,
                correlationId = correlationId);
            
        if result.errors.length() > 0 {
            log:printError("CSV processing failed with errors:", errors = result.errors, correlationId = correlationId);
        }

        return result;

    } on fail error e {
        return error(string `CSV processing failed: ${e.message()}`);
    }
}

// Clean up temporary file after processing 
function cleanupTempFile(string tempFilePath, string correlationId) returns error? {
    do {
        // Try to read file info to check if it exists and get size
        byte[]|io:Error fileContent = io:fileReadBytes(tempFilePath);

        if fileContent is io:Error {
            // File doesn't exist or can't be read
            log:printInfo(string `Temporary file does not exist or cannot be read, skipping cleanup: ${tempFilePath}`, correlationId = correlationId);
            return;
        }

        // Get file size for logging
        int fileSize = fileContent.length();
        log:printInfo(string `Found temporary file to cleanup: ${tempFilePath} (${fileSize} bytes)`, correlationId = correlationId);

        // Clear the file content from memory immediately after reading size
        fileContent = [];

        // Comprehensive file cleanup approach:
        // 1. Overwrite file with empty content to clear data and free disk space
        check file:remove(tempFilePath);

        // 2. Log successful cleanup
        log:printInfo(string `Successfully cleared temporary file content: ${tempFilePath} (freed ${fileSize} bytes)`, correlationId = correlationId);

        // 4. Additional cleanup for large files
        if fileSize > 1048576 { // Files larger than 1MB
            log:printInfo(string `Large temporary file cleanup completed: ${tempFilePath} (${fileSize} bytes freed)`, correlationId = correlationId);
            runtime:sleep(0.02); // Longer pause for large file cleanup
        }

    } on fail error e {
        log:printError(string `Failed to cleanup temporary file: ${tempFilePath}, error: ${e.message()}`,
                correlationId = correlationId, 'error = e);
        return; // Continue processing even if cleanup fails
    }
}

function logProcessingResult(ProcessingResult result, string fileName, string correlationId) {
    log:printInfo(string `File processing completed: ${fileName}, ` +
                string `total: ${result.totalRecords}, ` +
                string `successful: ${result.successfulRecords}, ` +
                string `failed: ${result.failedRecords}, ` +
                string `quarantined: ${result.quarantinedRecords}, ` +
                string `enriched: ${result.enrichedShipments.length()}, ` +
                string `ndjson files: ${result.ndjsonFiles.length()}, ` +
                string `db inserted: ${result.dbInsertedRecords}`,
            correlationId = correlationId);
}

// Generate NDJSON output from enriched shipments

// Helper function to extract base name from filename
function getFileBaseName(string fileName) returns string {
    int? dotIndex = fileName.lastIndexOf(".");
    return dotIndex is int && dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
}

// Log database operation result asynchronously
function logDatabaseResult(future<DatabaseResult> dbFuture, string fileName, string correlationId) returns DatabaseResult {
    DatabaseResult|error result = wait dbFuture;

    if result is DatabaseResult {
        string message = string `NDJSON storage ${result.success ? "successful" : "failed"}: ${fileName}, records: ${result.recordsInserted}`;
        if result.success {
            log:printInfo(message, correlationId = correlationId);
        } else {
            log:printError(string `${message}, error: ${result.errorMessage ?: "Unknown"}`, correlationId = correlationId);
        }
        return result;
    }

    log:printError(string `Async NDJSON storage failed: ${fileName}`, correlationId = correlationId, 'error = result);
    return {success: false, recordsInserted: 0, errorMessage: result.message()};
}

// Process a batch of CSV records with enrichment, async database storage
function processBatch(string[][] batch, string correlationId) returns ProcessingResult {
    ProcessingResult result = {
        totalRecords: batch.length(),
        successfulRecords: 0,
        failedRecords: 0,
        quarantinedRecords: 0,
        errors: [],
        enrichedShipments: [],
        ndjsonFiles: [],
        dbInsertedRecords: 0,
        sftpUploadedFiles: []
    };

    EnrichedShipment[] enrichedShipmentsForDb = [];

    // Process in smaller chunks to manage memory
    int chunkSize = 1000; // Process 1000 records at a time within each batch
    int totalRecords = batch.length();
    int processedCount = 0;

    // Process batch in chunks
    while processedCount < totalRecords {
        int endIndex = int:min(processedCount + chunkSize, totalRecords);
        string[][] chunk = batch.slice(processedCount, endIndex);

        // Convert chunk to ShipmentCsvRecord array
        ShipmentCsvRecord[] records = [];
        foreach string[] row in chunk {
            if row.length() >= 5 {
                ShipmentCsvRecord|error csvRecord = parseShipmentCsvRecord(row);
                if csvRecord is ShipmentCsvRecord {
                    records.push(csvRecord);
                } else {
                    string errorMsg = "Failed to parse CSV record: " + csvRecord.message();
                    log:printError(errorMsg, correlationId = correlationId);
                }
            } else {
                string errorMsg = "Invalid CSV row: insufficient columns";
                log:printError(errorMsg, correlationId = correlationId);
            }
        }

        // Process valid records
        processRecordsSync(records, correlationId, result, enrichedShipmentsForDb);

        processedCount = endIndex;

        // Clear processed chunk data to free memory
        records = [];

        // Log progress for large batches
        if totalRecords > 50 {
            log:printInfo(string `Processed ${processedCount}/${totalRecords} records in batch`, correlationId = correlationId);
        }
    }

    // Perform async database insertion
    if enableDatabaseStorage && enrichedShipmentsForDb.length() > 0 {
        DatabaseResult dbResult = insertAndNotifyEnrichedShipmentsAsync(enrichedShipmentsForDb, correlationId);
        if dbResult.success {
            result.dbInsertedRecords = dbResult.recordsInserted;
        } else {
            result.errors.push("Async database batch insert failed: " + (dbResult.errorMessage ?: "Unknown error"));
        }
    }

    return result;
}

// Process records synchronously 
function processRecordsSync(ShipmentCsvRecord[] records, string correlationId, ProcessingResult result, EnrichedShipment[] enrichedShipmentsForDb) {
    foreach ShipmentCsvRecord csvRecord in records {
        EnrichedShipment|error enrichedResult = processShipmentRecordAndEnrichData(csvRecord, correlationId);

        if enrichedResult is EnrichedShipment {
            result.successfulRecords += 1;
            result.enrichedShipments.push(enrichedResult);

            if enableDatabaseStorage {
                enrichedShipmentsForDb.push(enrichedResult);
            }
        } else {
            result.failedRecords += 1;
            result.errors.push("Failed to process shipment: " + csvRecord.shipmentId + ", error: " + enrichedResult.message());
        }
    }
}

// Helper function to process batch and enrich data
function processBatchAndAggregateResults(string[][] batch, string fileName, int batchNumber, string correlationId, ProcessingResult overallResult) {
    if batch.length() == 0 {
        log:printWarn(string `No records to process in batch ${batchNumber}`, correlationId = correlationId);
        return;
    }

    log:printInfo(string `Processing batch ${batchNumber} with ${batch.length()} records`, correlationId = correlationId);
    ProcessingResult batchResult = processBatch(batch, correlationId);
    aggregateBatchResults(overallResult, batchResult, correlationId);
}

// Initialize processing result record
function initializeProcessingResult() returns ProcessingResult {
    return {
        totalRecords: 0,
        successfulRecords: 0,
        failedRecords: 0,
        quarantinedRecords: 0,
        errors: [],
        enrichedShipments: [],
        ndjsonFiles: [],
        dbInsertedRecords: 0,
        sftpUploadedFiles: []
    };
}

// Aggregate batch results into overall result
function aggregateBatchResults(ProcessingResult overallResult, ProcessingResult batchResult, string correlationId) {
    overallResult.totalRecords += batchResult.totalRecords;
    overallResult.successfulRecords += batchResult.successfulRecords;
    overallResult.failedRecords += batchResult.failedRecords;
    overallResult.quarantinedRecords += batchResult.quarantinedRecords;
    overallResult.dbInsertedRecords += batchResult.dbInsertedRecords;

    foreach string batchError in batchResult.errors {
        overallResult.errors.push(batchError);
    }
}

// Rename processed file in SFTP location
public function renameProcessedFile(string originalFileName, string correlationId) returns error? {

    // Get FTP client
    ftp:Client ftpClient = check getFtpClient();

    // Generate new filename with timestamp
    int? dotIndex = originalFileName.lastIndexOf(".");
    string baseName = dotIndex is int && dotIndex > 0 ? originalFileName.substring(0, dotIndex) : originalFileName;
    string extension = dotIndex is int && dotIndex > 0 ? originalFileName.substring(dotIndex) : "";
    string newFileName = processedFilePrefix + baseName + time:utcToString(time:utcNow()) + extension;

    // Build file paths and rename
    string originalPath = ftpDirectory + "/" + originalFileName;
    string newPath = (processedFileDirectory != ftpDirectory ? processedFileDirectory : ftpDirectory) + "/" + newFileName;

    error? renameResult = trap ftpClient->rename(originalPath, newPath);
    if renameResult is error {
        log:printDebug(string `Failed to rename file ${originalFileName}: ${renameResult.message()}`,
                correlationId = correlationId, 'error = renameResult);
        return (); // Return without error
    }     log:printInfo("Renamed file: " + originalFileName + " to " + newFileName);

    return ();
}

// Parse CSV row into ShipmentCsvRecord
function parseShipmentCsvRecord(string[] row) returns ShipmentCsvRecord|error {
    if row.length() < 5 {
        return error("Insufficient columns in CSV row");
    }

    return {
        shipmentId: row[0],
        shipmentDate: row[1],
        productCode: row[2],
        email: row[3],
        shipmentStatus: row[4],
        orderId: row.length() > 5 ? row[5] : (),
        origin: row.length() > 6 ? row[6] : (),
        destination: row.length() > 7 ? row[7] : ()
    };
}

// Process individual shipment record with retry logic and return enriched data
function processShipmentRecordAndEnrichData(ShipmentCsvRecord csvRecord, string _parentCorrelationId) returns EnrichedShipment|error {
    string correlationId = uuid:createType4AsString();
    log:printInfo("Processing shipment record and enriching data", shipmentId = csvRecord.shipmentId, correlationId = correlationId);

    foreach int attempt in 1 ... maxRetryAttempts {
        Shipment|error shipmentResult = getShipmentById(csvRecord.shipmentId, correlationId);

        if shipmentResult is Shipment {
            EnrichedShipment enrichedShipment = enrichShipmentWithCsvData(shipmentResult, csvRecord, correlationId);

            if enableEnrichResponseLogging {
                logEnrichedShipment(enrichedShipment, correlationId);
            }
            return enrichedShipment;
        } 
        
        log:printWarn("Failed to get shipment by ID:", shipmentId = csvRecord.shipmentId, 'error = shipmentResult, correlationId = correlationId);

        if attempt < maxRetryAttempts {
            runtime:sleep(<decimal>retryDelaySeconds);
        } else {
            return error(string `Max retries reached for shipment: ${csvRecord.shipmentId}, error: ${shipmentResult.message()}`);
        }
    }

    return error(string `Failed to process shipment after ${maxRetryAttempts} attempts: ${csvRecord.shipmentId}`);
}

// Helper function to log enriched shipment details
function logEnrichedShipment(EnrichedShipment shipment, string correlationId) {
    log:printInfo(string `Enriched shipment: {shipmentId: "${shipment.shipmentId}", ` +
                string `orderId: "${shipment.orderId}", customerId: "${shipment.customerId}", ` +
                string `customerName: "${shipment.customerName}", status: ${shipment.status}}`,
            correlationId = correlationId);
}

// Get shipment by ID from API with proper error handling
function getShipmentById(string shipmentId, string correlationId) returns Shipment|error {
    map<string|string[]> headers = {
        "X-Correlation-ID": correlationId,
        "Content-Type": "application/json"
    };

    http:Response response = check shipmentApiClient->get(path = string `/shipments/${shipmentId}`, headers = headers);
    json payload = check response.getJsonPayload();

    if response.statusCode >= 400 {
        ErrorResponse|error errorResponse = payload.cloneWithType(ErrorResponse);
        string errorMsg = errorResponse is ErrorResponse ?
            string `API error: ${errorResponse.message} (code: ${errorResponse.code})` :
            string `API returned error status ${response.statusCode}`;
        return error(errorMsg);
    }

    return check payload.cloneWithType(Shipment);
}

// Function to publish shipment event to Kafka
public function publishShipmentEvent(EnrichedShipment payload, string correlationId) {
    do {
        ShipmentMessage kafkaMessage = {
            customerId: payload.customerId,
            customerName: payload.customerName,
            customerEmail: payload.customerEmail,
            shipmentId: payload.shipmentId,
            shipmentDate: payload.shipmentDate,
            products: payload.products,
            status: "received",
            timestamp: time:utcToString(time:utcNow())
        };

        kafka:AnydataProducerRecord producerRecord = {
            topic: kafkaTopic,
            key: kafkaMessage.shipmentId,
            value: kafkaMessage,
            headers: {
                "correlation-id": correlationId
            }
        };

        check kafkaProducer->send(producerRecord);
        log:printInfo("Published shipment event to Kafka", shipmentId = payload.shipmentId, correlationId = correlationId);
    }
    on fail error err {
        log:printError("Failed to publish shipment event to Kafka", 'error = err);
    }
}

