# BigQuery

This guide explains how to configure d8a to use Google BigQuery as your data warehouse. BigQuery is Google's cloud data warehouse that can handle large-scale analytics workloads.

:::caution Free Tier Available
BigQuery offers a free tier that includes:

- **Storage**: The first 10 GiB per month is free
- **Queries (analysis)**: The first 1 TiB of query data processed per month is free

For more details, see the [BigQuery free usage tier documentation](https://cloud.google.com/bigquery/pricing?hl=en#free-usage-tier).
:::

## What you need

Before configuring BigQuery, make sure you have:

- A Google Cloud Platform (GCP) account
- A GCP project with BigQuery API enabled
- A GCP project ID (you can find it in the Google Cloud Console)
- A BigQuery dataset

You'll also need to create a service account with BigQuery Admin permissions and download its credentials. We'll walk through that next.

## Getting credentials

D8a needs a service account to authenticate with BigQuery. Here's how to create one and get the credentials:

### Step 1: Create the service account

1. Open the [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project from the dropdown at the top
3. Go to **IAM & Admin** → **Service Accounts** (or search for "Service Accounts" in the top search bar)
4. Click the **+ CREATE SERVICE ACCOUNT** button
5. Give it a name (e.g., `d8a-bigquery`) and optionally add a description
6. Click **CREATE AND CONTINUE**

### Step 2: Grant permissions

1. In the "Grant this service account access to project" section, find and select the **BigQuery Admin** role
   - You can type "BigQuery Admin" in the role search box to find it quickly
2. Click **CONTINUE**, then **DONE**

:::note
This role is required because it grants d8a the necessary permissions to create and modify tables, write data, and manage BigQuery resources.
:::

### Step 3: Download the key

1. Find your newly created service account in the list and click on it
2. Go to the **KEYS** tab
3. Click **ADD KEY** → **Create new key**
4. Select **JSON** as the key type
5. Click **CREATE** — this will download a JSON file to your computer

:::caution Important
Keep credentials secure. This file contains credentials that allow access to your BigQuery data. You'll need to copy its contents into your configuration file next.
:::

## Configuring credentials

Now that you have the service account JSON file, it's time to add it to your d8a configuration.

### Step 1: Get your project ID and dataset name

- **Project ID**: You can find this in the Google Cloud Console at the top of the page, or in the downloaded JSON file (look for the `project_id` field).
- **Dataset Name**: The name of your BigQuery dataset. If you don't have one yet, [create it in the BigQuery console](https://docs.cloud.google.com/bigquery/docs/datasets#create-dataset).

### Step 2: Add the configuration

Choose the option that matches your installation type (Cloud or On-premises):

#### Option A: Cloud installation

Fill in the BigQuery settings in the d8a UI. Navigate to the BigQuery configuration section and provide the following fields:

- **Database name** \* (required)

  - Your name for the BigQuery connection that you will see in d8a UI

- **BigQuery Project ID** \* (required)

  - Your GCP project ID (mentioned in Step 1 above)

- **BigQuery Dataset ID** \* (required)

  - Your BigQuery dataset ID (mentioned in Step 1 above)

- **BigQuery Table Name** (optional)

  - The table in which you want to store the data (defaults to `events`)

- **BigQuery Credentials (JSON)** \* (required)
  - The whole content of the JSON file that you downloaded earlier (simply paste the file contents into the field)

**Verifying your setup:**

Go to the [BigQuery console](https://console.cloud.google.com/bigquery) and check if your table has been created with the proper d8a schema. The [database schema](/articles/database-schema) documentation describes all available columns.

#### Option B: On-premises installation

:::info Tip
Full configuration reference is available [here](/articles/config#--bigquery-creds-json).
:::

Open your `config.yaml` file and add the BigQuery configuration:

```yaml
warehouse:
  driver: bigquery
bigquery:
  project_id: your-gcp-project-id
  dataset_name: your-dataset-name
  # Paste the contents of the downloaded JSON file here
  creds_json: |
    {
      "type": "service_account",
      ...
    }
```

:::info Tip
Make sure the indentation is correct. The JSON should be indented with spaces to align under `creds_json:`. The `|` after `creds_json:` allows multi-line strings in YAML.
:::

**Verifying your setup:**

After configuring BigQuery, start d8a and check the logs. You should see messages indicating successful connection to BigQuery.

You can also go to the [BigQuery console](https://console.cloud.google.com/bigquery) and check if your table has been created with the proper d8a schema. The [database schema](/articles/database-schema) documentation describes all available columns.
