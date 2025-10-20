import React, { useState } from 'react';
import { ScenarioTemplate, SettingsValues, SubScenario } from './types';
import styles from './Scenarios.module.css';

// Pastel color palette for sessions
const sessionColors = [
  '#FFD6E0', // pastel pink
  '#FFEFB5', // pastel yellow
  '#BDFFD6', // pastel green
  '#C4E0FF', // pastel blue
  '#E6C9FF', // pastel purple
  '#FFD6C9', // pastel peach
  '#C9FFEE', // pastel mint
  '#DFDFFF', // pastel lavender
  '#FFE4BD', // pastel orange
  '#D6FFD6', // pastel lime
];

// SettingsToggleTable displays interactive settings with checkboxes
const SettingsToggleTable: React.FC<{
  template: ScenarioTemplate,
  settingsValues: SettingsValues,
  onSettingChange: (key: string, value: string) => void
}> = ({ template, settingsValues, onSettingChange }) => (
  <table className={styles.settingsTable}>
    <thead>
      <tr>
        <th>Setting</th>
        <th>Value</th>
      </tr>
    </thead>
    <tbody>
      {template.settings.map((setting) => (
        <tr key={setting.key}>
          <td>{setting.name}</td>
          <td>
            <label className={styles.checkboxLabel}>
              <input
                type="checkbox"
                checked={settingsValues[setting.key] === "yes"}
                onChange={(e) => onSettingChange(setting.key, e.target.checked ? "yes" : "no")}
              />
              <span className={styles.checkboxText}>
                {settingsValues[setting.key] === "yes" ? "Yes" : "No"}
              </span>
            </label>
          </td>
        </tr>
      ))}
    </tbody>
  </table>
);

// DeviceTable displays the events for a device
const DeviceTable: React.FC<{ 
  device: SubScenario['devices'][0], 
  sessions: SubScenario['sessions'] 
}> = ({ device, sessions }) => {
  if (!device.events.length) return null;

  // Create a mapping of event numbers to session colors
  const eventColorMap: Record<number, string> = {};
  
  // Assign colors to events based on their session
  sessions.forEach((session, sessionIndex) => {
    const colorIndex = sessionIndex % sessionColors.length;
    const sessionColor = sessionColors[colorIndex];
    
    // Map each event in the session to its color
    session.events.forEach(eventNumber => {
      eventColorMap[eventNumber] = sessionColor;
    });
  });

  return (
    <div>
      <h4>{device.name} events:</h4>
      <table className={styles.deviceTable}>
        <tbody>
          <tr>
            <th>#</th>
            {device.events.map((event, i) => (
              <td 
                key={i} 
                style={{ backgroundColor: eventColorMap[event.eventNumber] || 'transparent' }}
              >
                {event.eventNumber}
              </td>
            ))}
          </tr>
          <tr>
            <th>client_id</th>
            {device.events.map((event, i) => (
              <td 
                key={i}
                style={{ backgroundColor: eventColorMap[event.eventNumber] || 'transparent' }}
              >
                {event.client_id}
              </td>
            ))}
          </tr>
          <tr>
            <th>user_id</th>
            {device.events.map((event, i) => (
              <td 
                key={i}
                style={{ backgroundColor: eventColorMap[event.eventNumber] || 'transparent' }}
              >
                {event.user_id || '-'}
              </td>
            ))}
          </tr>
          <tr>
            <th>session_stamp</th>
            {device.events.map((event, i) => (
              <td 
                key={i}
                style={{ backgroundColor: eventColorMap[event.eventNumber] || 'transparent' }}
              >
                {event.session_stamp || '-'}
              </td>
            ))}
          </tr>
        </tbody>
      </table>
    </div>
  );
};

// SessionsList displays the sessions for a scenario
const SessionsList: React.FC<{ sessions: SubScenario['sessions'] }> = ({ sessions }) => (
  <div className={styles.sessionsContainer}>
    <p>Sessions:</p>
    <ul>
      {sessions.map((session, index) => {
        const colorIndex = index % sessionColors.length;
        const sessionColor = sessionColors[colorIndex];
        
        return (
          <li key={index}>
            <div style={{ 
              display: 'inline-block', 
              width: '12px', 
              height: '12px', 
              backgroundColor: sessionColor,
              marginRight: '8px',
              border: '1px solid #ccc'
            }}></div>
            Visitor ID: <b>{session.visitorId}</b>, from {session.source}
            <ul>
              <li>Events: {session.events.join(', ')}</li>
              {session.notes && <li>Notes: {session.notes}</li>}
            </ul>
          </li>
        );
      })}
    </ul>
  </div>
);

// Legend component to display session color associations
const SessionLegend: React.FC<{ sessions: SubScenario['sessions'] }> = ({ sessions }) => {
  if (!sessions.length) return null;
  
  return (
    <div className={styles.legendContainer}>
      <h4>Session Color Legend</h4>
      <div className={styles.legendItems}>
        {sessions.map((session, index) => {
          const colorIndex = index % sessionColors.length;
          const sessionColor = sessionColors[colorIndex];
          
          return (
            <div key={index} className={styles.legendItem}>
              <div 
                className={styles.colorSwatch}
                style={{ backgroundColor: sessionColor }}
              ></div>
              <span>Visitor: {session.visitorId}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
};

// ResultDisplay shows the result based on selected settings
const ResultDisplay: React.FC<{ result: SubScenario | null }> = ({ result }) => {
  if (!result) {
    return (
      <div className={styles.noResultContainer}>
        <p>No data available for this combination of settings.</p>
        <p>Try a different configuration.</p>
      </div>
    );
  }

  return (
    <div className={styles.resultContainer}>
      <h4>Result: {result.title}</h4>
      

      {result.devices.map((device, i) => (
        <DeviceTable key={i} device={device} sessions={result.sessions} />
      ))}

      <SessionsList sessions={result.sessions} />
    </div>
  );
};

// InteractiveScenarioComponent displays a scenario with interactive settings
const InteractiveScenarioComponent: React.FC<{ template: ScenarioTemplate }> = ({ template }) => {
  // Initialize settings with default values
  const initialSettings: SettingsValues = {};
  template.settings.forEach(setting => {
    initialSettings[setting.key] = setting.defaultValue;
  });

  const [settingsValues, setSettingsValues] = useState<SettingsValues>(initialSettings);

  // Handle setting change
  const handleSettingChange = (key: string, value: string) => {
    setSettingsValues(prev => ({
      ...prev,
      [key]: value
    }));
  };

  // Get the result based on current settings
  const result = template.getResult(settingsValues);

  return (
    <div className={styles.interactiveScenario}>
      <h3 id={`scenario-${template.id}`}>Scenario {template.id}: {template.title}</h3>
      <p>{template.description}</p>

      <div className={styles.scenarioContent}>
        <div className={styles.settingsContainer}>
          <h4>Settings</h4>
          <SettingsToggleTable
            template={template}
            settingsValues={settingsValues}
            onSettingChange={handleSettingChange}
          />
        </div>

        <ResultDisplay result={result} />
      </div>
    </div>
  );
};

// ScenarioTemplateList displays a clickable list of scenario templates
const ScenarioTemplateList: React.FC<{
  templates: ScenarioTemplate[],
  selectedTemplateId: string | null,
  onSelectTemplate: (id: string) => void
}> = ({ templates, selectedTemplateId, onSelectTemplate }) => (
  <div className={styles.scenarioList}>
    <ul>
      {templates.map(template => (
        <li key={template.id}>
          <button
            className={`${styles.scenarioButton} ${selectedTemplateId === template.id ? styles.active : ''}`}
            onClick={() => onSelectTemplate(template.id)}
          >
            Scenario {template.id}: {template.title}
          </button>
        </li>
      ))}
    </ul>
  </div>
);

// Interactive Scenarios component that shows all scenario templates
const InteractiveScenariosComponent: React.FC<{ templates: ScenarioTemplate[] }> = ({ templates }) => {
  const [selectedTemplateId, setSelectedTemplateId] = useState<string | null>(
    templates.length > 0 ? templates[0].id : null
  );

  const selectedTemplate = templates.find(t => t.id === selectedTemplateId);

  return (
    <div className="container">
      <div className="row">
        <div className="col col--3">
          <ScenarioTemplateList
            templates={templates}
            selectedTemplateId={selectedTemplateId}
            onSelectTemplate={setSelectedTemplateId}
          />
        </div>
        <div className="col col--9">
          {selectedTemplate ? (
            <InteractiveScenarioComponent template={selectedTemplate} />
          ) : (
            <p>Select a scenario to view details</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default InteractiveScenariosComponent; 