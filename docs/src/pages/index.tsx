import type { ReactNode } from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Translate, { translate } from "@docusaurus/Translate";
import Layout from "@theme/Layout";
import Heading from "@theme/Heading";

import styles from "./index.module.css";

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx("hero hero--primary", styles.heroBanner)}>
      <div className="container">
        <div className={styles.heroContent}>
          <Heading as="h1" className={styles.heroTitle}>
            {siteConfig.title}
          </Heading>
          <p className={styles.heroSubtitle}>
            <Translate id="homepage.hero.tagline">
              Own your analytics with d8a: open-source clickstream analytics
              compatible with GA4 and Matomo.
            </Translate>
          </p>
        </div>
      </div>
    </header>
  );
}

function QuickLinks() {
  return (
    <section className={styles.quickLinks}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          <Translate id="homepage.quickLinks.title">Quick Links</Translate>
        </Heading>
        <div className="row">
          <div className="col col--4">
            <div className={styles.linkCard}>
              <Heading as="h3" className={styles.cardTitle}>
                <Link to="/getting-started">
                  <Translate id="homepage.quickLinks.gettingStarted.title">
                    Getting Started
                  </Translate>
                </Link>
              </Heading>
              <p className={styles.cardDescription}>
                <Translate id="homepage.quickLinks.gettingStarted.description">
                  Set up d8a in minutes using Cloud or go On-premises with
                  Docker.
                </Translate>
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.linkCard}>
              <Heading as="h3" className={styles.cardTitle}>
                <Link to="/articles/warehouses">
                  <Translate id="homepage.quickLinks.warehouses.title">
                    Warehouses
                  </Translate>
                </Link>
              </Heading>
              <p className={styles.cardDescription}>
                <Translate id="homepage.quickLinks.warehouses.description">
                  Choose your destination: BigQuery, ClickHouse, or CSV files on
                  S3, GCP (GCS), and filesystem.
                </Translate>
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.linkCard}>
              <Heading as="h3" className={styles.cardTitle}>
                <Link to="/articles/sources/matomo">
                  <Translate id="homepage.quickLinks.sources.title">
                    Source Integrations
                  </Translate>
                </Link>
              </Heading>
              <p className={styles.cardDescription}>
                <Translate id="homepage.quickLinks.sources.description">
                  Connect GA4 and Matomo trackers to d8a with compatible source
                  ingestion endpoints.
                </Translate>
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function Features() {
  return (
    <section className={styles.features}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          <Translate id="homepage.features.title">Why d8a?</Translate>
        </Heading>
        <div className="row">
          <div className="col col--6">
            <div className={styles.featureItem}>
              <Heading as="h3" className={styles.featureTitle}>
                🔒{" "}
                <Translate id="homepage.features.hosting.title">
                  Hosting flexibility
                </Translate>
              </Heading>
              <p>
                <Translate id="homepage.features.hosting.description">
                  Keep your analytics data on your own infrastructure with
                  complete data sovereignty or use Cloud.
                </Translate>
              </p>
            </div>
          </div>
          <div className="col col--6">
            <div className={styles.featureItem}>
              <Heading as="h3" className={styles.featureTitle}>
                🔌{" "}
                <Translate id="homepage.features.sources.title">
                  Source Compatibility
                </Translate>
              </Heading>
              <p>
                <Translate id="homepage.features.sources.description">
                  Ingest events using GA4, Matomo, or the native d8a protocol
                  for fully independent on-prem analytics with a reporting-ready
                  data schema.
                </Translate>
              </p>
            </div>
          </div>
          <div className="col col--6">
            <div className={styles.featureItem}>
              <Heading as="h3" className={styles.featureTitle}>
                📊{" "}
                <Translate id="homepage.features.destinations.title">
                  Multiple Destinations
                </Translate>
              </Heading>
              <p>
                <Translate id="homepage.features.destinations.description">
                  Deliver analytics data to BigQuery, ClickHouse, and other
                  warehouses like Snowflake and Redshift through CSV on
                  S3/MinIO, GCP (GCS), and filesystem.
                </Translate>
              </p>
            </div>
          </div>
          <div className="col col--6">
            <div className={styles.featureItem}>
              <Heading as="h3" className={styles.featureTitle}>
                🚀{" "}
                <Translate id="homepage.features.openSource.title">
                  Open Source
                </Translate>
              </Heading>
              <p>
                <Translate id="homepage.features.openSource.description">
                  Fully open-source with an active community. Customize and
                  extend as needed.
                </Translate>
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title}`}
      description={translate({
        id: "homepage.layout.description",
        message: "Docs - d8a.tech - An Open Source Clickstream",
      })}
    >
      <HomepageHeader />
      <main>
        <QuickLinks />
        <Features />
      </main>
    </Layout>
  );
}
