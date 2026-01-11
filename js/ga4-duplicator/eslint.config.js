import js from "@eslint/js";
import eslintConfigPrettier from "eslint-config-prettier";
import globals from "globals";
import tseslintParser from "@typescript-eslint/parser";
import tseslintPlugin from "@typescript-eslint/eslint-plugin";

export default [
  {
    ignores: ["dist/**", "coverage/**"],
  },
  js.configs.recommended,
  {
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        ...globals.browser,
        ...globals.node,
        // Additional browser types for fetch API and XMLHttpRequest
        RequestInfo: "readonly",
        RequestInit: "readonly",
        XMLHttpRequestBodyInit: "readonly",
        BodyInit: "readonly",
      },
    },
    rules: {
      // Keep tests readable; underscore-prefix is treated as intentionally unused.
      "no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      // Allow console in JS sources too; we validate console output in tests.
      "no-console": "off",
      // Allow empty catch blocks for error suppression
      "no-empty": "off",
    },
  },
  {
    files: ["**/*.ts"],
    languageOptions: {
      parser: tseslintParser,
      parserOptions: {
        // Keep this non-type-aware for speed + incremental adoption.
        // We'll switch to project-aware linting later in the migration.
        ecmaVersion: "latest",
        sourceType: "module",
      },
    },
    plugins: {
      "@typescript-eslint": tseslintPlugin,
    },
    rules: {
      // Use the TS-aware unused-vars rule; keep underscore-prefix ignore behavior.
      "no-unused-vars": "off",
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],

      // During migration we don't want to force explicit return types everywhere.
      "@typescript-eslint/explicit-function-return-type": "off",

      // Keep console usage allowed in TS sources (debug logging is a feature).
      "@typescript-eslint/no-console": "off",
    },
  },
  // Disable ESLint formatting rules that conflict with Prettier.
  eslintConfigPrettier,
];
