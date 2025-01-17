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

    collections = [
        {
            "dataSourceName": source["name"],
            "totalCount": len(source.get("tables", [])), 
        }
        for source in metadata.get("sources", [])
        if source["kind"] == "mongo"
    ]

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
        fieldnames = [
            "Endpoint",
            "Source",
            "TotalTablesAndViews",
            "TotalCollections",
            "TotalLogicalModels",
            "TotalProjectModels",
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

       
        for row in data:
            for source in row["Sources"]:
                writer.writerow({
                    "Endpoint": row["Endpoint"],
                    "Source": source["SourceName"],
                    "TotalTablesAndViews": source["TablesAndViews"],
                    "TotalCollections": source["Collections"],
                    "TotalLogicalModels": source["LogicalModels"],
                    "TotalProjectModels": row["TotalProjectModels"],
                })

        # Calculate total counts
        total_tables_and_views = sum(
            source["TablesAndViews"]
            for row in data
            for source in row["Sources"]
        )
        total_collections = sum(
            source["Collections"]
            for row in data
            for source in row["Sources"]
        )
        total_logical_models = sum(
            source["LogicalModels"]
            for row in data
            for source in row["Sources"]
        )
        total_models = sum(row["TotalProjectModels"] for row in data)

        # Write total row
        writer.writerow({
            "Endpoint": "Total",
            "Source": "All",
            "TotalTablesAndViews": total_tables_and_views,
            "TotalCollections": total_collections,
            "TotalLogicalModels": total_logical_models,
            "TotalProjectModels": total_models,
        })

    print(f"Total Models: {total_models}")


def main():
 
    input_file = "endpoints.json"  
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

                result_data.append({
                    "Endpoint": api_url,
                    "Sources": [
                        {
                            "SourceName": table["dataSourceName"],
                            "TablesAndViews": table["totalCount"],
                            "Collections": next(
                                (
                                    coll["totalCount"]
                                    for coll in summary["collections"]
                                    if coll["dataSourceName"] == table["dataSourceName"]
                                ),
                                0,
                            ),
                            "LogicalModels": next(
                                (
                                    logical["totalCount"]
                                    for logical in summary["logicalModels"]
                                    if logical["dataSourceName"] == table["dataSourceName"]
                                ),
                                0,
                            ),
                        }
                        for table in summary["tablesAndViews"]
                    ],
                    "TotalProjectModels": summary["totalModels"],
                })

            except requests.exceptions.RequestException as e:
                print(f"Error fetching metadata for {api_url}: {e}")
                result_data.append({
                    "Endpoint": api_url,
                    "Sources": [
                        {
                            "SourceName": "Error",
                            "TablesAndViews": "Error",
                            "Collections": "Error",
                            "LogicalModels": "Error",
                        }
                    ],
                    "TotalProjectModels": "Error",
                })

        write_to_csv(result_data, output_file)
        print(f"Summary written to {output_file}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
