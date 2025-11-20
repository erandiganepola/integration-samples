import ballerina/http;

// Sample data arrays for generating mock shipments
string[] sampleCities = [
    "New York, NY",
    "Los Angeles, CA",
    "Chicago, IL",
    "Houston, TX",
    "Phoenix, AZ",
    "Memphis, TN",
    "Louisville, KY",
    "Baltimore, MD",
    "Milwaukee, WI",
    "Albuquerque, NM"
];

string[] sampleFirstNames = [
    "John",
    "Lisa",
    "Matthew",
    "Michelle",
    "Anthony",
    "Kimberly",
    "Mark",
    "Amy",
    "Donald",
    "Angela"
];

string[] sampleLastNames = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez"
];

string[] sampleCarriers = [
    "Express Logistics",
    "Fast Ship Co",
    "Reliable Transport",
    "Quick Delivery",
    "Standard Shipping",
    "Prime Express",
    "Global Freight",
    "Swift Cargo",
    "Rapid Transit"
];

string[] sampleProductCodes = [
    "XY123",
    "AB456",
    "CD789",
    "EF012",
    "HH456",
    "II789",
    "JJ012",
    "KK345",
    "LL678",
    "MM901"
];

ShipmentStatus[] sampleStatuses = [PENDING, PROCESSING, IN_TRANSIT, OUT_FOR_DELIVERY, DELIVERED, CANCELLED, RETURNED];

// HTTP service for mock shipment API
service /api/v1 on new http:Listener(8081) {

    // Get shipment by ID
    resource function get shipments/[string shipmentId]() returns Shipment|ShipmentNotFound|http:NotFound {
        if mockShipments.hasKey(shipmentId) {
            Shipment shipment = mockShipments.get(shipmentId);
            return shipment;
        } else {
            ShipmentNotFound notFoundResponse = {
                message: "Shipment not found",
                shipmentId: shipmentId
            };
            return notFoundResponse;
        }
    }

    // Get distinct shipment IDs from database
    resource function get shipments/ids() returns DistinctShipmentIdsResponse|http:InternalServerError {
        string[]|error shipmentIds = getDistinctShipmentIds();

        if shipmentIds is error {
            return http:INTERNAL_SERVER_ERROR;
        }

        DistinctShipmentIdsResponse response = {
            shipmentIds: shipmentIds,
            count: shipmentIds.length()
        };

        return response;
    }

}

// Function to generate mock shipments from SH001 to SH100 with multiple shipments per customer
function generateMockShipments() returns map<Shipment> {
    map<Shipment> shipments = {};

    // Create fewer unique customers so multiple shipments belong to same customer
    int totalCustomers = 35; // This will create ~2-3 shipments per customer on average

    int i = 1;
    while i <= 100 {
        string shipmentId = string `SH${i.toString().padZero(3)}`;
        string orderId = string `ORD${i.toString().padZero(3)}`;
        
        // Use modulo to assign multiple shipments to same customer
        int customerIndex = (i - 1) % totalCustomers + 1;
        string customerId = string `CUST${customerIndex.toString().padZero(3)}`;

        // Generate consistent customer name for the same customer ID
        string firstName = sampleFirstNames[customerIndex % sampleFirstNames.length()];
        string lastName = sampleLastNames[(customerIndex * 3) % sampleLastNames.length()];
        string customerName = string `${firstName} ${lastName}`;

        // Generate origin and destination
        string origin = sampleCities[i % sampleCities.length()];
        string destination = sampleCities[(i * 7) % sampleCities.length()];

        // Ensure origin and destination are different
        if origin == destination {
            destination = sampleCities[(i * 7 + 1) % sampleCities.length()];
        }

        // Generate status
        ShipmentStatus status = sampleStatuses[0];

        //ShipmentStatus status = sampleStatuses[i % sampleStatuses.length()];

        // Generate dates
        int dayOffset = (i % 30) + 1;
        string createdDate = string `2024-01-${dayOffset.toString().padZero(2)}`;

        int estimatedDays = dayOffset + 3 + (i % 7);
        string? estimatedDeliveryDate = ();
        if estimatedDays <= 31 {
            estimatedDeliveryDate = string `2024-01-${estimatedDays.toString().padZero(2)}`;
        } else {
            int febDay = estimatedDays - 31;
            estimatedDeliveryDate = string `2024-02-${febDay.toString().padZero(2)}`;
        }

        // Generate actual delivery date based on status
        string? actualDeliveryDate = ();
        if status == DELIVERED {
            // For delivered shipments, set actual delivery date
            int actualDays = dayOffset + 2 + (i % 5);
            if actualDays <= 31 {
                actualDeliveryDate = string `2024-01-${actualDays.toString().padZero(2)}`;
            } else {
                int febDay = actualDays - 31;
                actualDeliveryDate = string `2024-02-${febDay.toString().padZero(2)}`;
            }
        } else if status == CANCELLED {
            // For cancelled shipments, set actual delivery date to cancellation date
            int cancelDays = dayOffset + 1 + (i % 3);
            if cancelDays <= 31 {
                actualDeliveryDate = string `2024-01-${cancelDays.toString().padZero(2)}`;
            } else {
                int febDay = cancelDays - 31;
                actualDeliveryDate = string `2024-02-${febDay.toString().padZero(2)}`;
            }
        }
        else if status == PENDING {
            // For cancelled shipments, set actual delivery date to cancellation date
            int cancelDays = dayOffset + 1 + (i % 3);
            if cancelDays <= 31 {
                actualDeliveryDate = string `2024-01-${cancelDays.toString().padZero(2)}`;
            } else {
                int febDay = cancelDays - 31;
                actualDeliveryDate = string `2024-02-${febDay.toString().padZero(2)}`;
            }
        }
        else if status == PROCESSING {
            // For cancelled shipments, set actual delivery date to cancellation date
            int cancelDays = dayOffset + 1 + (i % 3);
            if cancelDays <= 31 {
                actualDeliveryDate = string `2024-01-${cancelDays.toString().padZero(2)}`;
            } else {
                int febDay = cancelDays - 31;
                actualDeliveryDate = string `2024-02-${febDay.toString().padZero(2)}`;
            }
        } else if status == RETURNED {
            // For returned shipments, set actual delivery date to return date
            int returnDays = dayOffset + 4 + (i % 6);
            if returnDays <= 31 {
                actualDeliveryDate = string `2024-01-${returnDays.toString().padZero(2)}`;
            } else {
                int febDay = returnDays - 31;
                actualDeliveryDate = string `2024-02-${febDay.toString().padZero(2)}`;
            }
        } else if status == OUT_FOR_DELIVERY {
            // For out for delivery, set expected delivery date (today or tomorrow)
            int outForDeliveryDays = dayOffset + 3 + (i % 2);
            if outForDeliveryDays <= 31 {
                actualDeliveryDate = string `2024-01-${outForDeliveryDays.toString().padZero(2)}`;
            } else {
                int febDay = outForDeliveryDays - 31;
                actualDeliveryDate = string `2024-02-${febDay.toString().padZero(2)}`;
            }
        } else if status == IN_TRANSIT {
            // For in transit, optionally set expected actual delivery date
            if i % 3 == 0 { // Only for some in-transit shipments
                int transitDays = dayOffset + 2 + (i % 4);
                if transitDays <= 31 {
                    actualDeliveryDate = string `2024-01-${transitDays.toString().padZero(2)}`;
                } else {
                    int febDay = transitDays - 31;
                    actualDeliveryDate = string `2024-02-${febDay.toString().padZero(2)}`;
                }
            }
        }
        // For PENDING and PROCESSING, actualDeliveryDate remains null

        // Generate weight
        decimal totalWeight = <decimal>(1.0 + (i % 50) * 0.1);

        // Generate carrier
        string carrier = sampleCarriers[i % sampleCarriers.length()];

        // Generate tracking number
        string trackingNumber = string `TRK${(1000000000 + i * 12345).toString()}`;

        // Generate products
        Product[] products = [];
        int productCount = (i % 3) + 1; // 1 to 3 products per shipment
        int j = 0;
        while j < productCount {
            string productCode = sampleProductCodes[(i + j) % sampleProductCodes.length()];
            int qty = 10 + ((i + j) % 20) * 5; // Quantities from 10 to 105
            products.push({productCode: productCode, qty: qty});
            j += 1;
        }

        // Create shipment record
        Shipment shipment = {
            shipmentId: shipmentId,
            orderId: orderId,
            customerId: customerId,
            customerName: customerName,
            origin: origin,
            destination: destination,
            status: status,
            createdDate: createdDate,
            estimatedDeliveryDate: estimatedDeliveryDate,
            actualDeliveryDate: actualDeliveryDate,
            totalWeight: totalWeight,
            carrier: carrier,
            trackingNumber: trackingNumber,
            products: products
        };

        shipments[shipmentId] = shipment;
        i += 1;
    }

    return shipments;
}

// Mock shipment data with comprehensive fields (SH001 to SH100)
map<Shipment> mockShipments = generateMockShipments();

public function main() returns error? {
    // Service will start automatically when the module is run
}
