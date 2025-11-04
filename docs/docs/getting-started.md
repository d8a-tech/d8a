---
sidebar_position: 1
---

# Getting started

Welcome to Divine Data (d8a)! This guide will help you get up and running with d8a, an open source clickstream analytics platform that's fully compatible with GA4 tracking protocols.

## Step 1: Install d8a

Follow the installation instructions in the [GitHub repository](https://github.com/d8a-tech/d8a?tab=readme-ov-file#getting-started) to install and run the d8a product.

The installation process will guide you through:
- Running the d8a server
- Making your first test request
- Verifying that data collection is working

Once you have d8a running and can successfully send test requests, you're ready to configure your existing GA4 setup.

## Step 2: Reconfigure Your GA4 Setup

After installing d8a, you'll need to reconfigure your current GA4 setup to send traffic to the d8a product. This allows you to capture analytics data using your own infrastructure while maintaining compatibility with GA4 tracking protocols.

Follow the [Intercepting GA4 Events](./intercepting-ga4-events) guide to configure your Google Tag Manager implementation to send data to your d8a endpoint.

The guide covers multiple methods:
- **Method 1 (Recommended)**: Duplicate GA4 requests to keep your existing GA4 setup intact while also sending data to d8a
- **Method 2**: Redirect all GA4 requests to d8a for complete control over your data

Choose the method that best fits your needs based on whether you want to continue sending data to Google Analytics or fully migrate to d8a.

## Next Steps

After completing both steps:
- Verify that events are being received by your d8a instance
- Review the [columns documentation](./columns) to understand the data schema
- Explore the [protocols documentation](./protocols/ga4) for detailed protocol information

