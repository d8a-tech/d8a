import React from 'react';
import CodeBlock from '@theme/CodeBlock';
import DynamicForm from './DynamicForm';
import { RJSFSchema, UiSchema } from '@rjsf/utils';

interface GA4DuplicatorSnippetGeneratorProps {
    ga4DuplicatorMin: string;
}

const schema: RJSFSchema = {
    type: 'object',
    properties: {
        isCloud: {
            type: 'boolean',
            title: 'Is this a d8a Cloud project?',
            default: true,
        },
        destinations: {
            type: 'array',
            title: '(optional) Mapp different measurement IDs to different endpoints',
            items: {
                type: 'object',
                properties: {
                    measurement_id: {
                        type: 'string',
                        title: 'Measurement ID (e.g. G-XXXX, or *)',
                    },
                    server_container_url: {
                        type: 'string',
                        title: 'Endpoint URL',
                    },
                },
                required: ['measurement_id', 'server_container_url'],
            },
        },
    },
    dependencies: {
        isCloud: {
            oneOf: [
                {
                    properties: {
                        isCloud: { enum: [true] },
                        property_id: {
                            type: 'string',
                            title: 'Property ID',
                        },
                    },
                    required: ['property_id'],
                },
                {
                    properties: {
                        isCloud: { enum: [false] },
                        endpoint_url: {
                            type: 'string',
                            title: 'Full Tracking Endpoint URL',
                            description: 'e.g. https://your-server.com/g/collect',
                        },
                    },
                    required: ['endpoint_url'],
                },
            ],
        },
    },
};

const uiSchema: UiSchema = {
    'ui:order': ['isCloud', 'property_id', 'endpoint_url', 'destinations'],
};

export default function GA4DuplicatorSnippetGenerator({
    ga4DuplicatorMin,
}: GA4DuplicatorSnippetGeneratorProps) {
    return (
        <DynamicForm schema={schema} uiSchema={uiSchema}>
            {(formData) => {
                let server_container_url = '';
                let server_container_path = undefined;

                if (formData.isCloud) {
                    if (formData.property_id) {
                        server_container_url = `https://global.t.d8a.tech/${formData.property_id}`;
                    } else {
                        server_container_url = 'https://global.t.d8a.tech/YOUR_PROPERTY_ID';
                    }
                } else if (formData.endpoint_url) {
                    try {
                        const url = new URL(formData.endpoint_url);
                        if (url.pathname.endsWith('/g/collect')) {
                            server_container_url =
                                url.origin + url.pathname.substring(0, url.pathname.length - '/g/collect'.length);
                            server_container_path = '/g/collect';
                        } else {
                            server_container_url = url.origin;
                            server_container_path = url.pathname === '/' ? undefined : url.pathname;
                        }
                    } catch {
                        server_container_url = formData.endpoint_url;
                    }
                }

                const config: any = {
                    server_container_url: server_container_url || 'https://your-endpoint.com',
                };
                if (server_container_path) {
                    config.server_container_path = server_container_path;
                }

                if (formData.destinations && formData.destinations.length > 0) {
                    config.destinations = formData.destinations.map((d: any) => {
                        const dest: any = { measurement_id: d.measurement_id };
                        try {
                            const url = new URL(d.server_container_url);
                            if (url.pathname.endsWith('/g/collect')) {
                                dest.server_container_url =
                                    url.origin + url.pathname.substring(0, url.pathname.length - '/g/collect'.length);
                                dest.server_container_path = '/g/collect';
                            } else {
                                dest.server_container_url = url.origin;
                                dest.server_container_path = url.pathname === '/' ? undefined : url.pathname;
                            }
                        } catch {
                            dest.server_container_url = d.server_container_url;
                        }
                        return dest;
                    });
                }

                const configJson = JSON.stringify(config, null, 2);

                return (
                    <CodeBlock language="html" title="GA4 Duplicator Snippet">
                        {`<script>
// GA4 Duplicator initialization
${ga4DuplicatorMin}

window.createGA4Duplicator(${configJson});
</script>`}
                    </CodeBlock>
                );
            }}
        </DynamicForm>
    );
}
