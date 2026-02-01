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

D8a supports two authentication methods for BigQuery:

- **OAuth 2.0**: Connect using your personal Google account. This grants write access to BigQuery across all your Google projects. Good for personal use.
- **Service Account**: Upload a service account JSON key file. Allows setting more granular permissions, good for organizations.

We'll walk through both options below.

## Getting credentials

D8a supports two authentication methods. Choose the one that best fits your needs:

### Option 1: OAuth 2.0 (Cloud-only)

OAuth 2.0 allows you to connect using your personal Google account. This method is simpler to set up and doesn't require creating a service account.

**How it works:**

1. In the d8a UI, click **Authorize with Google (OAuth 2)**
2. A new window will open asking you to sign in to your Google account
3. Review and grant the requested permissions
4. The window will close automatically and your integration will be created

:::warning
OAuth integrations are shared at the tenant level. Once you create an OAuth integration, it becomes available to all users in your tenant who have access to integrations. This means your colleagues can select the same integration and use it to configure their own BigQuery warehouse connections with different project, dataset, and table settings. The OAuth credentials grant write access to BigQuery across all Google projects associated with the authenticated Google account. Because of that, it's recommended to use OAuth 2.0 for personal tenants.
:::

### Option 2: Service Account (Cloud and On-premises)

Service accounts allow you to set more granular permissions and are better suited for organizational use. Here's how to create one:

#### Step 1: Create the service account

1. Open the [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project from the dropdown at the top
3. Go to **IAM & Admin** → **Service Accounts** (or search for "Service Accounts" in the top search bar)
4. Click the **+ CREATE SERVICE ACCOUNT** button
5. Give it a name (e.g., `d8a-bigquery`) and optionally add a description
6. Click **CREATE AND CONTINUE**

#### Step 2: Grant permissions

1. In the "Grant this service account access to project" section, find and select the **BigQuery Admin** role
   - You can type "BigQuery Admin" in the role search box to find it quickly
2. Click **CONTINUE**, then **DONE**

:::note
This role is required because it grants d8a the necessary permissions to create and modify tables, write data, and manage BigQuery resources.
:::

#### Step 3: Download the key

1. Find your newly created service account in the list and click on it
2. Go to the **KEYS** tab
3. Click **ADD KEY** → **Create new key**
4. Select **JSON** as the key type
5. Click **CREATE** — this will download a JSON file to your computer

:::caution Important
Keep credentials secure. This file contains credentials that allow access to your BigQuery data. You'll need to upload this file in the d8a UI next.
:::

## Configuring credentials

### Step 1: Get your project ID and dataset name

- **Project ID**: You can find this in the Google Cloud Console at the top of the page, or in the downloaded JSON file (look for the `project_id` field).
- **Dataset Name**: The name of your BigQuery dataset. If you don't have one yet, [create it in the BigQuery console](https://docs.cloud.google.com/bigquery/docs/datasets#create-dataset).

### Step 2: Add the configuration

Choose the option that matches your installation type (Cloud or On-premises):

#### Option A: Cloud installation

In the d8a UI, navigate to the BigQuery configuration section. You'll see two options for authentication:

**Using OAuth 2.0:**

1. Click **Authorize with Google (OAuth 2)**
2. Sign in to your Google account in the popup window
3. Review and grant the requested permissions
4. The window will close automatically once authorization is complete

**Using Service Account:**

1. Click **Choose JSON file** under "Upload Service Account JSON key"
2. Select the JSON file you downloaded earlier

After selecting your authentication method, click next and provide the following fields:

- **Database name** \* (required)

  - Your name for the BigQuery connection that you will see in d8a UI

- **BigQuery Project ID** \* (required)

  - Your GCP project ID (mentioned in Step 1 above)

- **BigQuery Dataset ID** \* (required)

  - Your BigQuery dataset ID (mentioned in Step 1 above)

- **BigQuery Table Name** (optional)

  - The table in which you want to store the data (defaults to `events`)

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
