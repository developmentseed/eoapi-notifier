#!/usr/bin/env python3
"""
CloudEvents HTTP endpoint for integration testing.

This server receives CloudEvents via HTTP and logs them for verification
in integration tests.
"""

import json
import time

import uvicorn
from cloudevents.http import from_http
from fastapi import FastAPI, Request, Response

app = FastAPI(title="CloudEvents Test Endpoint", version="1.0.0")

# Store received events for testing
received_events = []


@app.post("/")
@app.post("/events")
async def receive_cloudevent(request: Request):
    """Receive and process CloudEvents."""
    headers = dict(request.headers)
    body = await request.body()

    print("📡 Received CloudEvent request:")
    print(f"   Headers: {dict(headers)}")

    try:
        event = from_http(headers, body)

        event_data = {
            "source": event.get("source", "unknown"),
            "type": event.get("type", "unknown"),
            "subject": event.get("subject"),
            "data": event.data,
            "timestamp": time.time(),
        }

        received_events.append(event_data)

        print("✅ CloudEvent parsed successfully:")
        print(f"   Source: {event_data['source']}")
        print(f"   Type: {event_data['type']}")
        print(f"   Subject: {event_data['subject']}")
        print(f"   Data: {event_data['data']}")

        return Response(status_code=202, content="Event received")

    except Exception as e:
        error_msg = f"❌ CloudEvent parse error: {e}"
        print(error_msg)
        print(f"   Headers: {headers}")
        if body:
            try:
                print(f"   Body: {body.decode()}")
            except Exception:
                print(f"   Body (bytes): {body}")

        return Response(status_code=400, content=json.dumps({"error": str(e)}))


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ready",
        "timestamp": time.time(),
        "events_received": len(received_events),
    }


@app.get("/events")
async def get_events():
    """Get all received events for testing."""
    return {"count": len(received_events), "events": received_events}


@app.delete("/events")
async def clear_events():
    """Clear all received events."""
    global received_events
    count = len(received_events)
    received_events = []
    return {"cleared": count}


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "CloudEvents Test Endpoint",
        "status": "running",
        "endpoints": {
            "POST /": "Receive CloudEvents",
            "POST /events": "Receive CloudEvents",
            "GET /health": "Health check",
            "GET /events": "Get received events",
            "DELETE /events": "Clear received events",
        },
    }


if __name__ == "__main__":
    print("🚀 CloudEvents endpoint starting on :8080")
    print("📋 Available endpoints:")
    print("   POST /        - Receive CloudEvents")
    print("   POST /events  - Receive CloudEvents")
    print("   GET /health   - Health check")
    print("   GET /events   - Get received events")
    print("   DELETE /events - Clear received events")

    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
