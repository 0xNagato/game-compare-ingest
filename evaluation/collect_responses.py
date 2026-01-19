#!/usr/bin/env python3
"""
Response Collection Runner for i-miss-rust Evaluation

This script executes the test queries against the i-miss-rust application
and collects responses for evaluation purposes.
"""

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Any

# Configuration
QUERIES_FILE = Path("evaluation/test_queries.json")
RESPONSES_FILE = Path("evaluation/test_responses.json")
API_BASE_URL = "http://127.0.0.1:8080"  # Default API server address
TIMEOUT_SECONDS = 30


def load_queries() -> List[Dict[str, Any]]:
    """Load test queries from JSON file."""
    with open(QUERIES_FILE, 'r') as f:
        return json.load(f)


def run_api_query(query: Dict[str, Any]) -> Dict[str, Any]:
    """Execute an API query and collect the response."""
    import requests
    
    endpoint = query.get("endpoint", "/api/health")
    params = query.get("parameters", {})
    
    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
        
        return {
            "query_id": query["id"],
            "query_type": query["type"],
            "query": query["query"],
            "response_status": response.status_code,
            "response_data": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
            "response_time_ms": response.elapsed.total_seconds() * 1000,
            "success": response.ok,
            "timestamp": time.time()
        }
    except Exception as e:
        return {
            "query_id": query["id"],
            "query_type": query["type"],
            "query": query["query"],
            "error": str(e),
            "success": False,
            "timestamp": time.time()
        }


def run_provider_ingestion(query: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a provider ingestion and collect results."""
    provider = query.get("provider", "unknown")
    product_id = query.get("product_id", "")
    
    try:
        # Run the ingest binary for specific provider
        cmd = [
            "cargo", "run", "--bin", "gc", "--",
            "ingest", "--provider", provider,
            "--product-id", product_id
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SECONDS,
            cwd="/Users/lowkey/Desktop/i-miss-rust"
        )
        
        return {
            "query_id": query["id"],
            "query_type": query["type"],
            "query": query["query"],
            "provider": provider,
            "product_id": product_id,
            "exit_code": result.returncode,
            "stdout": result.stdout[-500:] if len(result.stdout) > 500 else result.stdout,  # Last 500 chars
            "stderr": result.stderr[-500:] if len(result.stderr) > 500 else result.stderr,
            "success": result.returncode == 0,
            "timestamp": time.time()
        }
    except subprocess.TimeoutExpired:
        return {
            "query_id": query["id"],
            "query_type": query["type"],
            "query": query["query"],
            "error": f"Timeout after {TIMEOUT_SECONDS}s",
            "success": False,
            "timestamp": time.time()
        }
    except Exception as e:
        return {
            "query_id": query["id"],
            "query_type": query["type"],
            "query": query["query"],
            "error": str(e),
            "success": False,
            "timestamp": time.time()
        }


def run_data_validation(query: Dict[str, Any]) -> Dict[str, Any]:
    """Execute data validation queries against the database."""
    # This would connect directly to PostgreSQL for validation
    # Placeholder for now
    return {
        "query_id": query["id"],
        "query_type": query["type"],
        "query": query["query"],
        "validation_result": "pending_implementation",
        "success": False,
        "timestamp": time.time()
    }


def collect_responses() -> List[Dict[str, Any]]:
    """Execute all queries and collect responses."""
    queries = load_queries()
    responses = []
    
    print(f"🔄 Collecting responses for {len(queries)} queries...")
    
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}/{len(queries)}] Processing: {query['id']} - {query['type']}")
        
        query_type = query.get("type", "")
        
        if "api_" in query_type:
            response = run_api_query(query)
        elif query_type == "provider_ingestion":
            response = run_provider_ingestion(query)
        elif query_type in ["data_normalization", "cross_provider_reconciliation", "currency_conversion"]:
            response = run_data_validation(query)
        else:
            response = {
                "query_id": query["id"],
                "query_type": query_type,
                "error": f"Unknown query type: {query_type}",
                "success": False,
                "timestamp": time.time()
            }
        
        responses.append(response)
        print(f"  Status: {'✅ Success' if response.get('success') else '❌ Failed'}")
        
        # Small delay between requests
        time.sleep(0.5)
    
    return responses


def save_responses(responses: List[Dict[str, Any]]) -> None:
    """Save collected responses to JSON file."""
    RESPONSES_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    with open(RESPONSES_FILE, 'w') as f:
        json.dump(responses, f, indent=2)
    
    print(f"\n✅ Saved {len(responses)} responses to {RESPONSES_FILE}")


def main():
    """Main execution function."""
    print("🚀 i-miss-rust Evaluation Response Collection")
    print("=" * 60)
    
    # Check if queries file exists
    if not QUERIES_FILE.exists():
        print(f"❌ Error: Queries file not found at {QUERIES_FILE}")
        sys.exit(1)
    
    # Collect responses
    try:
        responses = collect_responses()
        save_responses(responses)
        
        # Summary
        successful = sum(1 for r in responses if r.get("success"))
        print(f"\n📊 Summary:")
        print(f"  Total queries: {len(responses)}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {len(responses) - successful}")
        
    except KeyboardInterrupt:
        print("\n⚠️  Collection interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during collection: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
