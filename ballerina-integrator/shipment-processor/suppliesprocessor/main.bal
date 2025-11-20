import ballerina/ftp;
import ballerina/log;
import ballerina/uuid;
import ballerinax/wso2.controlplane as _;

// Global state for email tracking
int sentEmailCount = 0;

// FTP listener configuration for file polling
listener ftp:Listener ftpListener = new ({
    protocol: ftp:SFTP,
    host: ftpHost,
    port: ftpPort,
    auth: {
        credentials: {
            username: ftpUsername,
            password: ftpPassword
        }
    },
    path: ftpDirectory,
    pollingInterval: ftpListenerPollingInterval,
    fileNamePattern: ftpListenerFilePattern
});

// FTP service for processing incoming files
service on ftpListener {

    remote function onFileChange(ftp:WatchEvent event, ftp:Caller caller) returns error? {
        string correlationId = uuid:createType4AsString();

        // Process all added files with parallel error isolation
        foreach ftp:FileInfo addedFile in event.addedFiles {
            error? processResult = processFile(caller, addedFile, correlationId);
            if processResult is error {
                log:printError(string `Failed to process file ${addedFile.name}`,
                        correlationId = correlationId, 'error = processResult);
            }
        }
    }
}

