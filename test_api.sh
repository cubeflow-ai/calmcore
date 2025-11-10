#!/bin/bash

set -e

BASE_URL="http://localhost:9200"

echo "=== Testing Elasticsearch API ==="
echo ""

# Test 1: Health check
echo "1. Health check..."
curl -s "${BASE_URL}/" | jq .
echo ""

# Test 2: Create index
echo "2. Creating index 'products'..."
curl -s -X PUT "${BASE_URL}/products" \
  -H 'Content-Type: application/json' \
  -d '{
    "mappings": {
      "properties": {
        "_id": { "type": "keyword" },
        "name": { "type": "text" },
        "price": { "type": "double" },
        "category": { "type": "keyword" },
        "in_stock": { "type": "boolean" }
      }
    }
  }' | jq .
echo ""

# Test 3: Insert document with ID
echo "3. Inserting document with ID '1'..."
curl -s -X PUT "${BASE_URL}/products/_doc/1" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Laptop",
    "price": 999.99,
    "category": "electronics",
    "in_stock": true
  }' | jq .
echo ""

# Test 4: Get document
echo "4. Getting document with ID '1'..."
curl -s -X GET "${BASE_URL}/products/_doc/1" | jq .
echo ""

# Test 5: Bulk insert
echo "5. Bulk inserting documents..."
curl -s -X POST "${BASE_URL}/products/_bulk" \
  -H 'Content-Type: application/x-ndjson' \
  -d '{"index":{"_id":"2"}}
{"name":"Keyboard","price":79.99,"category":"electronics","in_stock":true}
{"index":{"_id":"3"}}
{"name":"Monitor","price":299.99,"category":"electronics","in_stock":false}
' | jq .
echo ""

# Test 6: Search documents
echo "6. Searching all documents..."
curl -s -X GET "${BASE_URL}/products/_search" | jq .
echo ""

# Test 7: List indices
echo "7. Listing all indices..."
curl -s -X GET "${BASE_URL}/_cat/indices" | jq .
echo ""

echo "=== All tests completed ==="
