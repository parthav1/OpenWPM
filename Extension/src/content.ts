import { injectJavascriptInstrumentPageScript } from "./content/javascript-instrument-content-scope";

const config = window.openWpmContentScriptConfig || {
  testing: false,
  jsInstrumentationSettings: [],
  useStealth: false,
};

if (config.useStealth) {
  console.log("OpenWPM: Initializing stealth JS instrumentation");
  // Stealth auto-initializes via its IIFE in stealth.ts
  import("./stealth/stealth");
} else {
  // Use regular instrumentation
  injectJavascriptInstrumentPageScript(config);
}

delete window.openWpmContentScriptConfig;