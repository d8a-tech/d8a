var klaroConfig = {
  acceptAll: true,
  mustConsent: true,
  hideDeclineAll: true,
  storageMethod: 'cookie',
  services: [
    {
      name: 'google-tag-manager',
      required: true,
      purposes: ['serviceprovision'],
      title: 'Google Tag Manager',
      description: 'This service manages the loading of other tracking services and ensures proper consent management. It is essential for the correct functioning of this website and cannot be disabled.',
      onAccept: `
        for(let k of Object.keys(opts.consents)){
          if (opts.consents[k]){
            let eventName = 'klaro-'+k+'-accepted'
            dataLayer.push({'event': eventName})
          }
        }
      `,
      onInit: `
        window.dataLayer = window.dataLayer || [];
        window.gtag = function(){dataLayer.push(arguments)};
        gtag('consent', 'default', {'ad_storage': 'denied', 'analytics_storage': 'denied', 'ad_user_data': 'denied', 'ad_personalization': 'denied'});
        gtag('set', 'ads_data_redaction', true);
      `,
    },
    {
      name: 'd8a',
      default: true,
      cookies: [/^.*_d8a(_.*)?/],
      purposes: ['analytics'],
      title: 'Divine Data',
      description: 'D8a helps us understand how visitors interact with our website by collecting and reporting information anonymously. This data helps us improve our services and user experience.',
      onAccept: `
        gtag('consent', 'update', { 
          'analytics_storage': 'granted',
        })
      `,
      onDecline: `
        gtag('consent', 'update', {
          'analytics_storage': 'denied',
        })
      `,
    },
    {
      name: 'google-analytics',
      default: true,
      cookies: [/^_ga(_.*)?/],
      purposes: ['analytics'],
      title: 'Google Analytics',
      description: 'Google Analytics helps us understand how visitors interact with our website by collecting and reporting information anonymously. This data helps us improve our services and user experience.',
      onAccept: `
        gtag('consent', 'update', {
          'analytics_storage': 'granted',
        })
      `,
      onDecline: `
        gtag('consent', 'update', {
          'analytics_storage': 'denied',
        })
      `,
    },
    {
      name: 'microsoft-clarity',
      default: true,
      cookies: [/^_cl.*/],
      purposes: ['analytics'],
      title: 'Microsoft Clarity',
      description: 'Microsoft Clarity helps us understand how visitors interact with our website by collecting and reporting information anonymously. This data helps us improve our services and user experience.',
      onAccept: `
        if(typeof window.clarity === "function") { window.clarity("consent"); }
      `,
      onDecline: `
        if(typeof window.clarity === "function") { window.clarity("consent", false); }
      `,
    },
    {
      name: 'google-ads',
      default: true,
      cookies: [],
      purposes: ['advertising'],
      title: 'Google Ads',
      description: 'We use Google Ads to measure the effectiveness of our advertising campaigns and to show relevant ads to users who have visited our website. This helps us optimize our marketing efforts.',
      onAccept: `
        gtag('consent', 'update', {
          'ad_storage': 'granted',
          'ad_user_data': 'granted',
          'ad_personalization': 'granted'
        })
      `,
      onDecline: `
        gtag('consent', 'update', {
          'ad_storage': 'denied',
          'ad_user_data': 'denied',
          'ad_personalization': 'denied'
        })
      `,
    }
  ],
  translations: {
    en: {
      privacyPolicyUrl: '/privacy',
      consentNotice: {
            description: 'This text will appear in the consent box.',
        },
        consentModal: {
            description:
                'Here you can see and customize the information that we collect about you.',
        },
        purposes: {
            serviceprovision: {
                title: 'Service provision',
                description: 'These services are necessary for the website to function properly and cannot be disabled.',
            },
            analytics: {
                title: 'Analytics'
            },
            advertising: {
                title: 'Advertising',
                description: '',
            },
        },

    }
  } 
}

if (typeof window !== 'undefined') {
  const isProduction = window.location.hostname.endsWith('.d8a.tech') || 
                       window.location.hostname === 'd8a.tech';
  
  window.klaroConfig = {
    ...klaroConfig,
    ...(isProduction ? { cookieDomain: '.d8a.tech' } : {}),
  };

  if (!document.querySelector('script[src*="klaro-no-css.js"]')) {
    const script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/klaro@latest/dist/klaro-no-css.js';
    script.async = true;
    document.head.appendChild(script);
  }
}

export function onRouteDidUpdate({ location, previousLocation }) {
  // No-op: Klaro handles consent management automatically
}
