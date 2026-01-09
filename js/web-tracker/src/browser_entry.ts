import { installD8a } from "./install.ts";

// Production bundle entrypoint (script-tag use for web-tracker).
// This must be side-effect only: it should install immediately on load.
installD8a();
