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
  children: (formData: any, isValid: boolean) => React.ReactNode;
}

export default function DynamicForm({
  schema,
  uiSchema,
  formData: initialFormData,
  children,
}: DynamicFormProps) {
  const [formData, setFormData] = useState(initialFormData || {});

  const validate = (data: any) => {
    if (!data || Object.keys(data).length === 0) return false;
    const result = validator.validateFormData(data, schema);
    return result.errors.length === 0;
  };

  const [isValid, setIsValid] = useState(() => validate(initialFormData));

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
                liveValidate
                onChange={(e) => {
                  setFormData(e.formData);
                  setIsValid(validate(e.formData));
                }}
              >
                <React.Fragment />
              </Form>
            </div>
          </div>
        </div>
        <div className="col col--6">
          {children(formData, isValid)}
        </div>
      </div>
    </div>
  );
}
