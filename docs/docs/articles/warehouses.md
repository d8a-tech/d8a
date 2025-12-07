# Warehouses

D8a supports multiple warehouse destinations for your analytics data. This flexibility allows you to choose the data warehouse that best fits your infrastructure, compliance requirements, and analytical needs.

Currently supported warehouses:
- **BigQuery**: Google's cloud data warehouse for large-scale analytics
- **ClickHouse**: Fast, open-source column-oriented database

## BigQuery

This guide explains how to configure d8a to use Google BigQuery as your data warehouse. BigQuery is Google's cloud data warehouse that can handle large-scale analytics workloads.

### What You Need

Before configuring BigQuery, make sure you have:
- A Google Cloud Platform (GCP) account
- A GCP project with BigQuery API enabled
- Your GCP project ID (you can find it in the Google Cloud Console)
- A BigQuery dataset

You'll also need to create a service account with BigQuery Admin permissions and download its credentials. We'll walk through that next.

### Getting Credentials

d8a needs a service account to authenticate with BigQuery. Here's how to create one and get the credentials:

#### Step 1: Create the Service Account

1. Open the [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project from the dropdown at the top
3. Go to **IAM & Admin** → **Service Accounts** (or search for "Service Accounts" in the top search bar)
4. Click the **+ CREATE SERVICE ACCOUNT** button
5. Give it a name (e.g., "d8a-bigquery-writer") and optionally a description
6. Click **CREATE AND CONTINUE**

#### Step 2: Grant Permissions

1. In the "Grant this service account access to project" section, find and select the **BigQuery Admin** role
   - You can type "BigQuery Admin" in the role search box to find it quickly
2. Click **CONTINUE**, then **DONE**

:::note
Why BigQuery Admin? This role allows d8a, among others, to create and modify tables, and write data. There is an open issue to research the minimal permissions needed for v1.0.0 and update this guide accordingly.
:::

#### Step 3: Download the Key

1. Find your newly created service account in the list and click on it
2. Go to the **KEYS** tab
3. Click **ADD KEY** → **Create new key**
4. Select **JSON** as the key type
5. Click **CREATE** - this will download a JSON file to your computer

**Important:** Keep this file secure! It contains credentials that allow access to your BigQuery data. You'll need to copy its contents into your configuration file next.

### Configuring Credentials

Now that you have the service account JSON file, it's time to add it to your d8a configuration.

#### Step 1: Get Your Project ID and Dataset Name

- **Project ID**: You can find this in the Google Cloud Console at the top of the page, or in the downloaded JSON file (look for the `project_id` field)
- **Dataset Name**: The name of your BigQuery dataset. If you don't have one yet, create it in the BigQuery console, or d8a will create it for you if the service account has permission

#### Step 2: Add Configuration to config.yaml

Open your `config.yaml` file and add the BigQuery configuration:

```yaml
warehouse: bigquery
bigquery:
  project_id: your-gcp-project-id
  dataset_name: your-dataset-name
  # Here paste the contents of the downloaded JSON file
  creds_json: |
    {
      "type": "service_account",
      ...
    }
```

:::info Tip
    Make sure the indentation is correct. The JSON should be indented with spaces to align under `creds_json:`. The `|` after `creds_json:` allows multi-line strings in YAML.
:::

### Verifying Your Setup

After configuring BigQuery, start d8a and check the logs. You should see messages indicating successful connection to BigQuery.


## ClickHouse

ClickHouse is a fast, open-source column-oriented database management system. Configuring d8a to use ClickHouse is straightforward and requires no external setup.

### Configuration

Add the following to your `config.yaml` file:

```yaml
warehouse: clickhouse
clickhouse:
  host: localhost
  port: "9000"
  database: d8a
  username: default
  password: "your-password"
```

**Configuration fields:**
- `host`: ClickHouse server hostname or IP address
- `port`: ClickHouse native protocol port (default: `9000`)
- `database`: Database name (d8a will create it if it doesn't exist)
- `username`: ClickHouse username
- `password`: ClickHouse password

### Important Notes

- **Engine Support**: d8a currently supports only the `MergeTree` engine. Tables are created with `ENGINE = MergeTree()`.
- **Distributed/Replicated Setups**: Distributed and Replicated table setups are not supported at the moment. Use a single ClickHouse instance.

### Verifying Your Setup

After configuring ClickHouse, start d8a and check the logs. You should see messages indicating successful connection to ClickHouse.