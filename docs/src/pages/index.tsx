import type { ReactNode } from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <div className={styles.heroContent}>
          <Heading as="h1" className={styles.heroTitle}>
            {siteConfig.title}
          </Heading>
          <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
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
          Quick Links
        </Heading>
        <div className="row">
          <div className="col col--4">
            <div className={styles.linkCard}>
              <Heading as="h3" className={styles.cardTitle}>
                <Link to="/getting-started">Getting Started</Link>
              </Heading>
              <p className={styles.cardDescription}>
                Set up d8a in minutes with Docker and start tracking events right away.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.linkCard}>
              <Heading as="h3" className={styles.cardTitle}>
                <Link to="/articles/warehouses">Warehouses</Link>
              </Heading>
              <p className={styles.cardDescription}>
                Choose your data warehouse: BigQuery or ClickHouse. Configure and deploy.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.linkCard}>
              <Heading as="h3" className={styles.cardTitle}>
                <Link to="/guides/intercepting-ga4-events">GA4 Integration</Link>
              </Heading>
              <p className={styles.cardDescription}>
                Learn how to intercept and route GA4 events to your d8a instance.
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
          Why d8a?
        </Heading>
        <div className="row">
          <div className="col col--6">
            <div className={styles.featureItem}>
              <Heading as="h3" className={styles.featureTitle}>🔒 Hosting flexibility</Heading>
              <p>Keep your analytics data on your own infrastructure with complete data sovereignty or use Cloud.</p>
            </div>
          </div>
          <div className="col col--6">
            <div className={styles.featureItem}>
              <Heading as="h3" className={styles.featureTitle}>🔌 GA4 Compatible</Heading>
              <p>Use your existing GA4 tracking code without modification. A drop-in replacement for Google Analytics with a reporting-ready data schema.</p>
            </div>
          </div>
          <div className="col col--6">
            <div className={styles.featureItem}>
              <Heading as="h3" className={styles.featureTitle}>📊 Multiple Warehouses</Heading>
              <p>Store data in BigQuery or ClickHouse warehouses.</p>
            </div>
          </div>
          <div className="col col--6">
            <div className={styles.featureItem}>
              <Heading as="h3" className={styles.featureTitle}>🚀 Open Source</Heading>
              <p>Fully open-source with an active community. Customize and extend as needed.</p>
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
      description="Docs - d8a.tech - An Open Source Clickstream">
      <HomepageHeader />
      <main>
        <QuickLinks />
        <Features />
      </main>
    </Layout>
  );
}
