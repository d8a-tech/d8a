// ScenarioTypes.ts - Type definitions for scenario data

export interface Setting {
  name: string;
  value: string | boolean;
}

// New interface for interactive settings
export interface InteractiveSetting {
  name: string;
  key: string; // unique identifier for the setting
  options: string[]; // possible values, typically ["yes", "no"]
  defaultValue: string;
}

export interface DeviceEvent {
  eventNumber: number;
  client_id: string;
  user_id: string | null;
  session_stamp: string | null;
}

export interface Session {
  visitorId: string; // Visitor ID associated with this session
  source: string; // e.g. "client_id", "ss"
  events: number[];
  notes: string | null;
}

export interface Device {
  name: string;
  events: DeviceEvent[];
}

export interface SubScenario {
  id: string;
  title: string;
  settings: Setting[];
  devices: Device[];
  sessions: Session[];
}

// New interface for scenario templates
export interface ScenarioTemplate {
  id: string;
  title: string;
  description: string;
  settings: InteractiveSetting[];
  getResult: (settingsValues: Record<string, string>) => SubScenario | null;
}

export interface Scenario {
  id: string;
  title: string;
  subScenarios: SubScenario[];
}

// User-selected settings
export type SettingsValues = Record<string, string>; 