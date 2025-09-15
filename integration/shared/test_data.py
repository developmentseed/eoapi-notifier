#!/usr/bin/env python3
"""
Centralized test data definitions for integration testing.

Single source of truth for all test collections and items.
"""

import json
from datetime import UTC, datetime
from typing import Any

# TestData constants are defined locally in this module
pass


def get_test_collections() -> list[dict[str, Any]]:
    """Get test collections for integration testing."""
    return [
        {
            "id": "test-collection",
            "type": "Collection",
            "title": "Test Collection",
            "description": "A test collection for integration testing",
            "stac_version": "1.0.0",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2023-01-01T00:00:00Z", None]]},
            },
            "license": "proprietary",
        },
        {
            "id": "sample-collection",
            "type": "Collection",
            "title": "Sample Collection",
            "description": "Another collection for testing",
            "stac_version": "1.0.0",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2023-01-01T00:00:00Z", None]]},
            },
            "license": "proprietary",
        },
    ]


def get_test_items() -> list[dict[str, Any]]:
    """Get test items for integration testing."""
    return [
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "test-item-1",
            "collection": "test-collection",
            "geometry": {"type": "Point", "coordinates": [-122.4194, 37.7749]},
            "bbox": [-122.4194, 37.7749, -122.4194, 37.7749],
            "properties": {
                "datetime": "2023-01-01T12:00:00Z",
                "platform": "test",
                "instruments": ["test-sensor"],
            },
            "assets": {
                "thumbnail": {
                    "href": "https://example.com/thumb.jpg",
                    "type": "image/jpeg",
                }
            },
        },
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "test-item-2",
            "collection": "test-collection",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.5, 37.7],
                        [-122.4, 37.7],
                        [-122.4, 37.8],
                        [-122.5, 37.8],
                        [-122.5, 37.7],
                    ]
                ],
            },
            "bbox": [-122.5, 37.7, -122.4, 37.8],
            "properties": {
                "datetime": "2023-01-02T12:00:00Z",
                "platform": "test",
                "instruments": ["test-sensor"],
            },
            "assets": {
                "thumbnail": {
                    "href": "https://example.com/thumb2.jpg",
                    "type": "image/jpeg",
                }
            },
        },
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "sample-item-1",
            "collection": "sample-collection",
            "geometry": {"type": "Point", "coordinates": [-121.4944, 38.5816]},
            "bbox": [-121.4944, 38.5816, -121.4944, 38.5816],
            "properties": {
                "datetime": "2023-01-03T12:00:00Z",
                "platform": "sample",
                "instruments": ["sample-sensor"],
            },
            "assets": {
                "data": {"href": "https://example.com/data.tif", "type": "image/tiff"}
            },
        },
    ]


def create_test_item(
    item_id: str, collection: str = "test-collection"
) -> dict[str, Any]:
    """Create a test item with given ID and collection."""
    datetime_map = {
        "test-collection": "2023-01-01T18:00:00.000000Z",
        "sample-collection": "2023-01-03T12:00:00.000000Z",
    }

    datetime_str = datetime_map.get(collection, "2023-01-01T12:00:00.000000Z")

    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": item_id,
        "collection": collection,
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 0, 0],
        "properties": {
            "datetime": datetime_str,
            "test": True,
        },
        "assets": {},
    }


def get_datetime_for_collection(collection: str) -> datetime:
    """Get appropriate datetime for collection based on partition constraints."""
    if collection == "test-collection":
        return datetime(2023, 1, 1, 18, 0, 0, tzinfo=UTC)
    elif collection == "sample-collection":
        return datetime(2023, 1, 3, 12, 0, 0, tzinfo=UTC)
    else:
        return datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)


def get_collections_sql() -> str:
    """Get SQL to insert test collections."""
    collections = get_test_collections()
    values = []

    for collection in collections:
        collection_json = json.dumps(collection).replace("'", "''")
        values.append(f"('{collection_json}'::jsonb)")

    return f"""
    INSERT INTO pgstac.collections (content) VALUES
    {", ".join(values)}
    ON CONFLICT (id) DO NOTHING;
    """
