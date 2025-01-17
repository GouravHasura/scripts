
# Hasura Model Summary 

This script fetches metadata from multiple Hasura endpoints, calculates the total number of models (tables, views, collections, and logical models), and outputs the results into a CSV file.

## Steps to Use

1. **Add Your Endpoints:**
   - Add your Hasura GraphQL endpoints and their corresponding admin secrets to the `endpoints.json` file. The structure should be as follows:

   ```json
   [
     {
       "endpoint": "https://endpoint1.hasura.app/v1/graphql",
       "secret": "your-admin-secret-1"
     },
     {
       "endpoint": "https://endpoint2.hasura.app/v1/graphql",
       "secret": "your-admin-secret-2"
     }
   ]
   ```

2. **Run the Script:**
   - Execute the script with the following command:
   
   ```bash
   python3 getModels.py
   ```

3. **Check the CSV File:**
   - After running the script, check the generated CSV file for the results. It will contain the summarized model count for each endpoint.

## Output CSV Format

The CSV file will include the following columns:
- **Endpoint**: The Hasura API endpoint.
- **DataSource Name**: The name of the data source.
- **Model Type**: The type of model (Tables, Views, Collections, Logical Models).
- **Model Count**: The total count for each model type.
