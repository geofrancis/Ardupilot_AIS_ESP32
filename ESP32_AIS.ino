#include <ArduinoJson.h>
#include <WebSocketsClient.h>
#include <WiFi.h>
#include <time.h>

#include "mavlink/common/mavlink.h"

#include <algorithm>
#include <cmath>
#include <cstring>

// ========================================================================
// CONFIGURATION
// ========================================================================
const char* ssid = "2.4";
const char* password = "password";

//https://aisstream.io
const char* ais_api_key = "*******************************";

const int LED_PIN = 22;
unsigned long lastPacketFlashMs = 0;
const unsigned long FLASH_DURATION = 50;

unsigned long lastPrintTime = 0;
const unsigned long PRINT_INTERVAL = 5000;
bool displayUpdatePending = false;

// MAVLink
const uint8_t MAV_SYS_ID = 1;
const uint8_t MAV_COMP_ID = 191;
unsigned long lastHeartbeatTime = 0;
const unsigned long HEARTBEAT_INTERVAL = 1000;

// Debug mode: insert a synthetic vessel 10 miles east of ArduPilot GPS
const bool DEBUG_INSERT_VESSEL = true;
const double DEBUG_VESSEL_MILES_EAST = 10.0;
const uint32_t DEBUG_VESSEL_MMSI = 999000001; // synthetic MMSI for debug
const char DEBUG_VESSEL_NAME[] = "DEBUG_EAST_10MI";

// ========================================================================
// GPS STATE & THRESHOLDS
// ========================================================================
bool gps_ready = false;
float current_lat = NAN, current_lon = NAN;
float last_sent_lat = NAN, last_sent_lon = NAN;
const float MOVE_THRESHOLD_M = 100.0f;          // placeholder server update threshold
const float RECENTER_THRESHOLD_M = 1609.344f;   // 1 mile in meters

// ========================================================================
// Haversine distance
// ========================================================================
float distanceMeters(float lat1, float lon1, float lat2, float lon2) {
    if (isnan(lat1) || isnan(lon1) || isnan(lat2) || isnan(lon2)) return INFINITY;
    const float R = 6371000.0f;
    float dLat = radians(lat2 - lat1);
    float dLon = radians(lon2 - lon1);
    float a = sin(dLat/2.0f)*sin(dLat/2.0f) +
              cos(radians(lat1))*cos(radians(lat2)) *
              sin(dLon/2.0f)*sin(dLon/2.0f);
    float c = 2.0f * atan2(sqrt(a), sqrt(1.0f - a));
    return R * c;
}

// ========================================================================
// AIS SERVICE with dynamic bounding box centered on ArduPilot GPS
// ========================================================================
namespace services::ais {

constexpr size_t kMaxVessels = 64;
struct Vessel {
    uint32_t mmsi;
    char name[32];
    char type[16];
    char speed[16];
    float sog_knots;
    float lat;
    float lon;
    float heading_deg;
    unsigned long last_seen_ms;
};

namespace {
    constexpr char kHost[] = "stream.aisstream.io";
    constexpr uint16_t kPort = 443;
    constexpr char kPath[] = "/v0/stream";
    WebSocketsClient s_ws;
    Vessel s_vessels[kMaxVessels];
    size_t s_vessel_count = 0;
    char s_api_key[65] = "";
    bool s_started = false;
    bool s_connected = false;
    bool s_dirty = false;
    bool s_subscription_pending = false;
    char s_status_text[48] = "AIS OFFLINE";

    double s_center_lat = 0.0;
    double s_center_lon = 0.0;
    double s_radius_miles = 0.0;

    void setStatus(const char* text) { strncpy(s_status_text, text, 47); s_dirty = true; }
    bool hasApiKey() { return s_api_key[0] != '\0'; }

    Vessel* findOrAllocateVessel(uint32_t mmsi) {
        for (size_t i = 0; i < s_vessel_count; ++i)
            if (s_vessels[i].mmsi == mmsi) return &s_vessels[i];
        if (s_vessel_count < kMaxVessels) {
            Vessel* v = &s_vessels[s_vessel_count++];
            memset(v, 0, sizeof(*v));
            v->mmsi = mmsi;
            return v;
        }
        return &s_vessels[0];
    }

    void handleTextMessage(const char* payload) {
        StaticJsonDocument<1024> doc;
        DeserializationError err = deserializeJson(doc, payload);
        if (err) return;
        const char* type = doc["MessageType"] | "";
        JsonObject body = doc["Message"][type];
        if (body.isNull()) return;

        uint32_t mmsi = body["UserID"] | 0;
        Vessel* v = findOrAllocateVessel(mmsi);
        v->lat = body["Latitude"] | 0.0f;
        v->lon = body["Longitude"] | 0.0f;
        if (body.containsKey("TrueHeading")) v->heading_deg = body["TrueHeading"].as<float>();
        if (body.containsKey("Sog")) v->sog_knots = body["Sog"].as<float>();
        if (doc["MetaData"]["ShipName"].is<const char*>())
            strncpy(v->name, doc["MetaData"]["ShipName"], sizeof(v->name) - 1);
        v->last_seen_ms = millis();
        s_dirty = true;
    }

    void onWebSocketEvent(WStype_t type, uint8_t* payload, size_t length) {
        if (type == WStype_CONNECTED) {
            s_connected = true;
            s_subscription_pending = true;
            setStatus("AIS CONNECTED");
            Serial.println("AIS WebSocket connected");
        } else if (type == WStype_DISCONNECTED) {
            s_connected = false;
            setStatus("AIS DISCONNECTED");
            Serial.println("AIS WebSocket disconnected");
        } else if (type == WStype_TEXT) {
            handleTextMessage((char*)payload);
        }
    }

    String buildSubscriptionPayload() {
        double radius_km = s_radius_miles * 1.609344;
        double lat_offset = radius_km / 111.32;
        double lat_rad = s_center_lat * M_PI / 180.0;
        double lon_km_per_deg = 111.32 * cos(lat_rad);
        if (lon_km_per_deg <= 0.000001) lon_km_per_deg = 0.000001;
        double lon_offset = radius_km / lon_km_per_deg;

        double lat_min = s_center_lat - lat_offset;
        double lat_max = s_center_lat + lat_offset;
        double lon_min = s_center_lon - lon_offset;
        double lon_max = s_center_lon + lon_offset;

        String payload = "{\"APIKey\":\"";
        payload += s_api_key;
        payload += "\",\"BoundingBoxes\":[[[";
        payload += String(lat_min, 6);
        payload += ",";
        payload += String(lon_min, 6);
        payload += "],[";
        payload += String(lat_max, 6);
        payload += ",";
        payload += String(lon_max, 6);
        payload += "]]]}";
        return payload;
    }
}

void init(const char* key) { strncpy(s_api_key, key, 64); }

void setBoundingBox(double center_lat, double center_lon, double radius_miles) {
    s_center_lat = center_lat;
    s_center_lon = center_lon;
    s_radius_miles = radius_miles;
    s_subscription_pending = true;
    Serial.printf("Queued AIS bounding box center=%.6f,%.6f radius=%.1fmi\n", center_lat, center_lon, radius_miles);
}

// Insert or update a debug vessel at given lat/lon
void insertOrUpdateDebugVessel(uint32_t mmsi, const char* name, double lat, double lon) {
    // find existing
    for (size_t i = 0; i < s_vessel_count; ++i) {
        if (s_vessels[i].mmsi == mmsi) {
            s_vessels[i].lat = lat;
            s_vessels[i].lon = lon;
            s_vessels[i].last_seen_ms = millis();
            s_dirty = true;
            return;
        }
    }
    // allocate new
    if (s_vessel_count < kMaxVessels) {
        Vessel* v = &s_vessels[s_vessel_count++];
        memset(v, 0, sizeof(*v));
        v->mmsi = mmsi;
        strncpy(v->name, name, sizeof(v->name) - 1);
        v->lat = lat;
        v->lon = lon;
        v->last_seen_ms = millis();
        s_dirty = true;
        Serial.printf("Inserted debug vessel %s at %.6f, %.6f\n", name, lat, lon);
    } else {
        Serial.println("AIS vessel list full, cannot insert debug vessel");
    }
}

void loop() {
    if (WiFi.status() == WL_CONNECTED && hasApiKey()) {
        if (!s_started) {
            s_ws.beginSSL(kHost, kPort, kPath);
            s_ws.onEvent(onWebSocketEvent);
            s_started = true;
            Serial.println("AIS WebSocket started");
        }
        s_ws.loop();
        if (s_subscription_pending) {
            String payload = buildSubscriptionPayload();
            Serial.print("Sending AIS subscription: ");
            Serial.println(payload);
            s_ws.sendTXT(payload);
            s_subscription_pending = false;
        }
    }
}

size_t vesselCount() { return s_vessel_count; }
const Vessel* vesselList() { return s_vessels; }
bool consumeDirty() { bool d = s_dirty; s_dirty = false; return d; }

} // namespace services::ais

// ========================================================================
// MAVLINK parsing and sending
// ========================================================================
void readMavlink() {
    while (Serial2.available()) {
        uint8_t c = Serial2.read();
        mavlink_message_t msg;
        mavlink_status_t status;
        if (mavlink_parse_char(MAVLINK_COMM_0, c, &msg, &status)) {
            switch (msg.msgid) {
                case MAVLINK_MSG_ID_GLOBAL_POSITION_INT: {
                    mavlink_global_position_int_t pos;
                    mavlink_msg_global_position_int_decode(&msg, &pos);
                    current_lat = pos.lat / 1e7f;
                    current_lon = pos.lon / 1e7f;
                    if (!gps_ready) {
                        gps_ready = true;
                        last_sent_lat = current_lat;
                        last_sent_lon = current_lon;
                        services::ais::setBoundingBox(current_lat, current_lon, 50.0);
                        Serial.printf("GPS FIX ACQUIRED GLOBAL_POSITION_INT %.7f, %.7f\n", current_lat, current_lon);

                        // Insert debug vessel 10 miles east if enabled
                        if (DEBUG_INSERT_VESSEL) {
                            double lat_rad = current_lat * M_PI / 180.0;
                            double lon_km_per_deg = 111.32 * cos(lat_rad);
                            if (lon_km_per_deg <= 0.000001) lon_km_per_deg = 0.000001;
                            double delta_deg_lon = (DEBUG_VESSEL_MILES_EAST * 1.609344) / lon_km_per_deg;
                            double debug_lon = current_lon + delta_deg_lon;
                            double debug_lat = current_lat;
                            services::ais::insertOrUpdateDebugVessel(DEBUG_VESSEL_MMSI, DEBUG_VESSEL_NAME, debug_lat, debug_lon);
                        }
                    }
                    break;
                }
                case MAVLINK_MSG_ID_GPS_RAW_INT: {
                    mavlink_gps_raw_int_t gps;
                    mavlink_msg_gps_raw_int_decode(&msg, &gps);
                    if (gps.fix_type >= 3) {
                        current_lat = gps.lat / 1e7f;
                        current_lon = gps.lon / 1e7f;
                        if (!gps_ready) {
                            gps_ready = true;
                            last_sent_lat = current_lat;
                            last_sent_lon = current_lon;
                            services::ais::setBoundingBox(current_lat, current_lon, 50.0);
                            Serial.printf("GPS FIX ACQUIRED GPS_RAW_INT %.7f, %.7f\n", current_lat, current_lon);

                            // Insert debug vessel 10 miles east if enabled
                            if (DEBUG_INSERT_VESSEL) {
                                double lat_rad = current_lat * M_PI / 180.0;
                                double lon_km_per_deg = 111.32 * cos(lat_rad);
                                if (lon_km_per_deg <= 0.000001) lon_km_per_deg = 0.000001;
                                double delta_deg_lon = (DEBUG_VESSEL_MILES_EAST * 1.609344) / lon_km_per_deg;
                                double debug_lon = current_lon + delta_deg_lon;
                                double debug_lat = current_lat;
                                services::ais::insertOrUpdateDebugVessel(DEBUG_VESSEL_MMSI, DEBUG_VESSEL_NAME, debug_lat, debug_lon);
                            }
                        } else {
                            // If GPS already ready and debug enabled, update debug vessel position so it remains 10 miles east
                            if (DEBUG_INSERT_VESSEL) {
                                double lat_rad = current_lat * M_PI / 180.0;
                                double lon_km_per_deg = 111.32 * cos(lat_rad);
                                if (lon_km_per_deg <= 0.000001) lon_km_per_deg = 0.000001;
                                double delta_deg_lon = (DEBUG_VESSEL_MILES_EAST * 1.609344) / lon_km_per_deg;
                                double debug_lon = current_lon + delta_deg_lon;
                                double debug_lat = current_lat;
                                services::ais::insertOrUpdateDebugVessel(DEBUG_VESSEL_MMSI, DEBUG_VESSEL_NAME, debug_lat, debug_lon);
                            }
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }
}

void sendMavlinkHeartbeat() {
    mavlink_message_t msg;
    uint8_t buf[MAVLINK_MAX_PACKET_LEN];
    mavlink_msg_heartbeat_pack(
        MAV_SYS_ID, MAV_COMP_ID, &msg,
        MAV_TYPE_ONBOARD_CONTROLLER,
        MAV_AUTOPILOT_INVALID,
        0, 0, MAV_STATE_ACTIVE
    );
    uint16_t len = mavlink_msg_to_send_buffer(buf, &msg);
    Serial2.write(buf, len);
}

void sendMavlinkAisData() {
    size_t count = services::ais::vesselCount();
    const services::ais::Vessel* list = services::ais::vesselList();
    for (size_t i = 0; i < count; i++) {
        const auto& v = list[i];
        mavlink_message_t msg;
        uint8_t buf[MAVLINK_MAX_PACKET_LEN];
        mavlink_msg_ais_vessel_pack(
            MAV_SYS_ID, MAV_COMP_ID, &msg,
            v.mmsi,
            (int32_t)(v.lat * 1e7),
            (int32_t)(v.lon * 1e7),
            0,
            (uint16_t)(v.heading_deg * 100.0),
            (uint16_t)(v.sog_knots * 100.0),
            0, 0, 0,
            0, 0, 0, 0,
            "UNKNOWN",
            v.name,
            0,
            0
        );
        uint16_t len = mavlink_msg_to_send_buffer(buf, &msg);
        Serial2.write(buf, len);
    }
}

// ========================================================================
// UI & LED
// ========================================================================
void printVesselTable() {
    Serial.println("\n--- TRACKING ZONE ---");
    for (size_t i = 0; i < services::ais::vesselCount(); i++) {
        auto& v = services::ais::vesselList()[i];
        Serial.printf("MMSI: %lu | Name: %-15.15s | Pos: %.6f, %.6f | Hdg: %.1f | Spd: %.1f\n",
                      (unsigned long)v.mmsi, v.name, v.lat, v.lon, v.heading_deg, v.sog_knots);
    }
    Serial.println(">>> MAVLink AIS data sent to flight controller <<<");
}

void updateDiagnosticLED() {
    unsigned long now = millis();
    if (now - lastPacketFlashMs < FLASH_DURATION) digitalWrite(LED_PIN, HIGH);
    else if (services::ais::vesselCount() > 0) digitalWrite(LED_PIN, HIGH);
    else if (WiFi.status() != WL_CONNECTED) digitalWrite(LED_PIN, (now % 200 < 100));
    else digitalWrite(LED_PIN, LOW);
}

// ========================================================================
// Setup and main loop
// ========================================================================
void setup() {
    Serial.begin(115200);
    delay(50);
    Serial.println();
    Serial.println("=== BOOT ===");

  Serial2.begin(57600, SERIAL_8N1, 16, 17);     Serial.println("Serial2 started for MAVLink");

    pinMode(LED_PIN, OUTPUT);

    Serial.printf("Connecting to WiFi SSID: %s\n", ssid);
    WiFi.begin(ssid, password);
    unsigned long start = millis();
    const unsigned long WIFI_TIMEOUT_MS = 15000;
    while (WiFi.status() != WL_CONNECTED && (millis() - start) < WIFI_TIMEOUT_MS) {
        delay(250);
        digitalWrite(LED_PIN, !digitalRead(LED_PIN));
    }
    if (WiFi.status() == WL_CONNECTED) {
        Serial.print("WiFi connected, IP: ");
        Serial.println(WiFi.localIP());
    } else {
        Serial.println("WiFi not connected after timeout, continuing without WiFi");
    }

    services::ais::init(ais_api_key);
    Serial.println("Setup complete");
}

void loop() {
    unsigned long now = millis();

    // Read MAVLink messages
    readMavlink();

    // Wait for GPS fix before subscribing or recentering
    static unsigned long lastGpsWaitPrint = 0;
    if (!gps_ready) {
        updateDiagnosticLED();
        if (now - lastGpsWaitPrint > 5000) {
            Serial.println("Waiting for GPS fix...");
            lastGpsWaitPrint = now;
        }
        return;
    }

    // Server update placeholder (>100m)
    float dist_server = distanceMeters(last_sent_lat, last_sent_lon, current_lat, current_lon);
    if (dist_server > MOVE_THRESHOLD_M) {
        Serial.printf("Moved %.1f m since last server update — placeholder action\n", dist_server);
        last_sent_lat = current_lat;
        last_sent_lon = current_lon;
    }

    // Recenter AIS bounding box every 1 mile from last center
    static float last_center_lat = NAN;
    static float last_center_lon = NAN;
    if (isnan(last_center_lat)) {
        last_center_lat = current_lat;
        last_center_lon = current_lon;
    }
    float dist_center = distanceMeters(last_center_lat, last_center_lon, current_lat, current_lon);
    if (dist_center > RECENTER_THRESHOLD_M) {
        Serial.printf("Moved %.1f m from AIS center — recentering AIS bounding box to %.6f, %.6f\n",
                      dist_center, current_lat, current_lon);
        services::ais::setBoundingBox(current_lat, current_lon, 50.0); // 50 miles radius
        last_center_lat = current_lat;
        last_center_lon = current_lon;
    }

    // Heartbeat
    if (now - lastHeartbeatTime >= HEARTBEAT_INTERVAL) {
        sendMavlinkHeartbeat();
        lastHeartbeatTime = now;
    }

    // AIS loop
    services::ais::loop();
    if (services::ais::consumeDirty()) {
        lastPacketFlashMs = now;
        displayUpdatePending = true;
    }

    updateDiagnosticLED();

    if (displayUpdatePending && (now - lastPrintTime >= PRINT_INTERVAL)) {
        printVesselTable();
        sendMavlinkAisData();
        displayUpdatePending = false;
        lastPrintTime = now;
    }

    // Periodic status print
    static unsigned long lastStatus = 0;
    if (now - lastStatus > 10000) {
        Serial.printf("Status: gps_ready=%d vessels=%u wifi=%d lat=%.6f lon=%.6f\n",
                      gps_ready ? 1 : 0, (unsigned)services::ais::vesselCount(),
                      WiFi.status() == WL_CONNECTED, current_lat, current_lon);
        lastStatus = now;
    }
}
