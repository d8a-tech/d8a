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
            title: 'Are you using d8a Cloud?',
            default: true,
        },
        destinations: {
            type: 'array',
            title:
                'Optional: Use the button below to route specific GA4 Measurement IDs to different endpoints.',
            items: {
                type: 'object',
                properties: {
                    measurement_id: {
                        type: 'string',
                        title: 'Measurement ID:',
                        description: 'Example: G-XXXX or wildcard (*)',
                        minLength: 1,
                    },
                    server_container_url: {
                        type: 'string',
                        title: 'Tracking URL:',
                        description: 'Example: https://your-server.com/g/collect',
                        minLength: 1,
                    },
                },
                required: ['measurement_id', 'server_container_url'],
            },
        },
    },
    required: ['isCloud'],
    if: {
        properties: { isCloud: { const: true } },
    },
    then: {
        properties: {
            property_id: {
                type: 'string',
                title: 'Property ID:',
                description: 'You will find it in the d8a Cloud dashboard.',
                minLength: 1,
            },
        },
        required: ['property_id'],
    },
    else: {
        properties: {
            endpoint_url: {
                type: 'string',
                title: 'Tracking URL',
                description: 'Example: https://your-server.com/g/collect',
                minLength: 1,
            },
        },
        required: ['endpoint_url'],
    },
};

const uiSchema: UiSchema = {
    'ui:order': ['isCloud', 'property_id', 'endpoint_url', 'destinations'],
    destinations: {

        'ui:options': {
            orderable: false,
            addable: true,
            removable: true,
        },
        items: {
            'ui:options': {
                label: false,
            },
        },
    },
};

export default function GA4DuplicatorSnippetGenerator({
    ga4DuplicatorMin,
}: GA4DuplicatorSnippetGeneratorProps) {
    return (
        <DynamicForm
            schema={schema}
            uiSchema={uiSchema}
            formData={{ isCloud: true }}
        >
            {(formData, isValid) => {
                if (!isValid) {
                    return (
                        <div className="alert alert--warning" role="alert">
                            Fill in the required fields to generate the snippet, then click “Copy to clipboard”.
                        </div>
                    );
                }

                let server_container_url = '';

                if (formData.isCloud) {
                    if (formData.property_id) {
                        server_container_url = `https://global.t.d8a.tech/${formData.property_id}/g/collect`;
                    } else {
                        server_container_url = 'https://global.t.d8a.tech/YOUR_PROPERTY_ID/g/collect';
                    }
                } else if (formData.endpoint_url) {
                    server_container_url = formData.endpoint_url;
                }

                const config: any = {
                    server_container_url: server_container_url || 'https://your-endpoint.com/g/collect',
                };

                if (formData.destinations && formData.destinations.length > 0) {
                    config.destinations = formData.destinations.map((d: any) => {
                        return {
                            measurement_id: d.measurement_id,
                            server_container_url: d.server_container_url,
                        };
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
