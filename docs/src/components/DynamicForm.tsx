import React, { useState } from 'react';
import Form from '@rjsf/core';
import { RJSFSchema, UiSchema } from '@rjsf/utils';
import validator from '@rjsf/validator-ajv8';
import styles from './DynamicForm.module.css';
import clsx from 'clsx';

interface DynamicFormProps {
  schema: RJSFSchema;
  uiSchema?: UiSchema;
  formData?: any;
  children: (formData: any) => React.ReactNode;
}

export default function DynamicForm({
  schema,
  uiSchema,
  formData: initialFormData,
  children,
}: DynamicFormProps) {
  const [formData, setFormData] = useState(initialFormData || {});

  return (
    <div className={clsx("dynamic-form-container", styles.formContainer)}>
      <div className="row">
        <div className="col col--6">
          <div className="card shadow--lw" style={{ height: '100%', border: 'none' }}>
            <div className="card__body">
              <Form
                schema={schema}
                uiSchema={uiSchema}
                validator={validator}
                formData={formData}
                onChange={(e) => setFormData(e.formData)}
              >
                <React.Fragment />
              </Form>
            </div>
          </div>
        </div>
        <div className="col col--6">
          {children(formData)}
        </div>
      </div>
    </div>
  );
}
