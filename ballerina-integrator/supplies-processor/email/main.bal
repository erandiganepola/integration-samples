import ballerina/log;
import ballerinax/kafka;
import ballerinax/wso2.controlplane as _;

// Kafka service to consume shipment messages and send emails
listener kafka:Listener kafkaLis = new kafka:Listener(
    bootstrapServers = kafkaBootstrapServers,
    groupId = "shipment-email-service",
    topics = [kafkaTopic],
    securityProtocol = kafka:PROTOCOL_PLAINTEXT
);

service on kafkaLis {
    remote function onConsumerRecord(kafka:AnydataConsumerRecord[] messages) returns error? {
        foreach kafka:AnydataConsumerRecord currentMessage in messages {
            // Extract correlation-id from headers
            string? correlationId = check getCorelationId(currentMessage.headers);

            // Handle the message value conversion
            ShipmentMessage shipmentMessage = check getShipmentRecord(currentMessage.value);

            log:printInfo("Processing shipment message and sending email",
                    shipmentId = shipmentMessage.shipmentId,
                    correlationId = correlationId
            );

            // Send email notification
            error? emailResult = sendShipmentNotificationEmail(shipmentMessage, correlationId);
            if emailResult is error {
                log:printError("Failed to send email notification",
                        'error = emailResult,
                        shipmentId = shipmentMessage.shipmentId,
                        correlationId = correlationId
                );
                return emailResult;
            }
            log:printInfo("Email sent successfully",
                    shipmentId = shipmentMessage.shipmentId,
                    recipient = shipmentMessage.customerEmail,
                    correlationId = correlationId
            );
        }
    }

    remote function onError(kafka:Error kafkaError) returns error? {
        log:printError("Kafka consumer error occurred", 'error = kafkaError);
    }
}
