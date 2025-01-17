import requests
import json
import csv

def fetch_metadata(api_url, admin_secret):
    if api_url.endswith("/graphql"):
        api_url = api_url.replace("/graphql", "/metadata")
    
    headers = {
        "accept": "*/*",
        "content-type": "application/json",
        "x-hasura-admin-secret": admin_secret,
    }
    payload = {
        "type": "export_metadata",
        "version": 2,
        "args": {},
    }

    response = requests.post(api_url, headers=headers, json=payload)
    response.raise_for_status()  # Raise an error for HTTP failures
    return response.json()


def calculate_model_summary(metadata):
    tables_and_views = [
        {
            "dataSourceName": source["name"],
            "totalCount": len(source.get("tables", [])),
        }
        for source in metadata.get("sources", [])
        if source["kind"] != "mongo"
    ]

    # Extract collections for Mongo sources
    collections = [
        {
            "dataSourceName": source["name"],
            "totalCount": len(source.get("tables", [])),  # Mongo collections are tracked as "tables"
        }
        for source in metadata.get("sources", [])
        if source["kind"] == "mongo"
    ]

    # Extract logical models for non-Mongo sources
    logical_models = [
        {
            "dataSourceName": source["name"],
            "totalCount": len(source.get("logical_models", [])),
        }
        for source in metadata.get("sources", [])
        if source["kind"] != "mongo"
    ]

    def calculate_total(items):
        return sum(item["totalCount"] for item in items)

    # Calculate totals
    total_tables_and_views = calculate_total(tables_and_views)
    total_collections = calculate_total(collections)
    total_logical_models = calculate_total(logical_models)
    total_models = total_tables_and_views + total_collections + total_logical_models

    # Return summary
    return {
        "tablesAndViews": tables_and_views,
        "collections": collections,
        "logicalModels": logical_models,
        "totalModels": total_models,
    }


def write_to_csv(data, output_file):
    with open(output_file, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["Endpoint", "TotalTablesAndViews", "TotalCollections", "TotalLogicalModels", "TotalModels"])
        writer.writeheader()
        writer.writerows(data)

        # Adding a total row for all models
        total_tables_and_views = sum(row["TotalTablesAndViews"] for row in data)
        total_collections = sum(row["TotalCollections"] for row in data)
        total_logical_models = sum(row["TotalLogicalModels"] for row in data)
        total_models = sum(row["TotalModels"] for row in data)

        writer.writerow({
            "Endpoint": "Total",
            "TotalTablesAndViews": total_tables_and_views,
            "TotalCollections": total_collections,
            "TotalLogicalModels": total_logical_models,
            "TotalModels": total_models
        })

    # Print total models in the terminal
    print(f"Total Models: {total_models}")


def main():
    # Load endpoints and secrets from a JSON file
    input_file = "endpoints.json"  # Update with your actual file path
    output_file = "metadata_summary.csv"

    try:
        with open(input_file, "r") as file:
            endpoints = json.load(file)

        result_data = []

        for entry in endpoints:
            api_url = entry.get("endpoint")
            admin_secret = entry.get("secret")

            try:
                # Fetch metadata for each endpoint
                metadata_response = fetch_metadata(api_url, admin_secret)

                # Calculate model summary
                summary = calculate_model_summary(metadata_response["metadata"])

                # Append results to the list
                result_data.append({
                    "Endpoint": api_url,
                    "TotalTablesAndViews": sum(item["totalCount"] for item in summary["tablesAndViews"]),
                    "TotalCollections": sum(item["totalCount"] for item in summary["collections"]),
                    "TotalLogicalModels": sum(item["totalCount"] for item in summary["logicalModels"]),
                    "TotalModels": summary["totalModels"]
                })

            except requests.exceptions.RequestException as e:
                print(f"Error fetching metadata for {api_url}: {e}")
                result_data.append({
                    "Endpoint": api_url,
                    "TotalTablesAndViews": "Error",
                    "TotalCollections": "Error",
                    "TotalLogicalModels": "Error",
                    "TotalModels": "Error"
                })

        # Write results to a CSV file
        write_to_csv(result_data, output_file)
        print(f"Summary written to {output_file}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
