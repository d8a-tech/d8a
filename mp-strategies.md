<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# how to test js code that monkey patches window, fetch, document objects and things like that? I mean that code being tested does the patching.

Now I have enough information to provide a comprehensive answer. Let me create a detailed guide on testing code that monkey patches global objects.

## Testing JavaScript Code That Monkey Patches Global Objects

When testing code that modifies global objects like `window`, `fetch`, or `document`, the key challenge is ensuring **isolation** and **cleanup** to prevent test pollution. Here's how to do it effectively:

### Core Testing Strategies

**Using Spy Functions (Recommended for Jest/Vitest)**

The most robust approach uses spy functions which automatically track calls and allow cleanup. For Jest, use `jest.spyOn()`, and for Vitest, use `vi.spyOn()`:[^1_1][^1_2]

```javascript
// Jest example
test('patches fetch', () => {
  const originalFetch = global.fetch;
  jest.spyOn(global, 'fetch').mockResolvedValueOnce(
    new Response('{"data": "test"}')
  );
  
  // Your code that calls fetch
  expect(global.fetch).toHaveBeenCalled();
  
  // Cleanup
  jest.restoreAllMocks();
});
```

```javascript
// Vitest example
test('patches fetch', () => {
  vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
    new Response('{"data": "test"}')
  );
  
  expect(globalThis.fetch).toHaveBeenCalled();
});
```

**Global Stub Approach (Vitest)**

Vitest provides `vi.stubGlobal()` for a cleaner syntax:[^1_3][^1_1]

```javascript
import { vi, test, expect, afterEach } from 'vitest';

test('stubbing global fetch', () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
    new Response('{"message": "Success"}')
  ));
  
  const response = await fetch('https://api.example.com/data');
  expect(globalThis.fetch).toHaveBeenCalled();
});

afterEach(() => {
  vi.unstubAllGlobals();
});
```


### Cleanup and Isolation

**Automatic Cleanup with Test Hooks**

Always use `afterEach()` or `afterAll()` to restore original values:[^1_4][^1_5]

```javascript
describe('global monkey patching', () => {
  afterEach(() => {
    // For Jest
    jest.restoreAllMocks();
    jest.useRealTimers();
    
    // For Vitest
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  test('first test with mocked fetch', async () => {
    // test code
  });

  test('second test has clean globals', async () => {
    // test code - receives original fetch
  });
});
```

**Manual Restoration (When Needed)**

If you need fine-grained control, store and restore original values manually:[^1_4]

```javascript
test('manually restore globals', () => {
  const originalFetch = globalThis.fetch;
  const originalWindow = globalThis.window;
  
  try {
    globalThis.fetch = vi.fn().mockResolvedValue(new Response());
    // test code
    expect(globalThis.fetch).toHaveBeenCalled();
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.window = originalWindow;
  }
});
```


### Testing Code That Does the Patching

If your code itself performs the monkey patching (e.g., during initialization), test the patching behavior directly:[^1_6]

```javascript
// Code being tested
export function patchFetch() {
  const originalFetch = window.fetch;
  window.fetch = async (...args) => {
    console.log('Intercepting fetch:', args);
    return originalFetch(...args);
  };
  // Store original for detection
  window.__originalFetch = originalFetch;
}

// Test
test('code patches fetch correctly', () => {
  const mockFetch = vi.fn();
  vi.stubGlobal('fetch', mockFetch);
  
  patchFetch();
  
  expect(globalThis.__originalFetch).toBeDefined();
  expect(globalThis.fetch).not.toBe(mockFetch);
});
```


### Special Case: Testing Fake Timers

When patching timer-related functionality, use dedicated fake timer management:[^1_5][^1_7]

```javascript
describe('with fake timers', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    // or for Vitest:
    // vi.useFakeTimers();
  });

  afterEach(() => {
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
    // or for Vitest:
    // vi.runOnlyPendingTimers();
    // vi.useRealTimers();
  });

  test('setTimeout patching', () => {
    const callback = jest.fn();
    setTimeout(callback, 1000);
    
    jest.advanceTimersByTime(1000);
    expect(callback).toHaveBeenCalled();
  });
});
```


### Best Practices

**Isolation:** Always test patches in isolation from other tests. Never assume clean global state between tests.[^1_6]

**Assertions Before Cleanup:** Place your assertions before cleanup code, since cleanup modifies the globals.[^1_4]

**Document Patches:** If your code adds custom properties (like `window.__isFetchMonkeyPatched`), document them clearly and test their presence.[^1_8]

**Use Environment-Appropriate APIs:** In browser environments, use `window` and `globalThis`. In Node/Vitest, prefer `globalThis` over `global` for consistency.[^1_1][^1_3]

**Avoid Patching Testing Framework APIs:** Never patch functions used by the testing framework itself (like built-in assertions), as this can break the test infrastructure.[^1_9]

### Complete Example

```javascript
import { describe, test, expect, beforeEach, afterEach, vi } from 'vitest';

export function initializePatches() {
  // Your patching code
  const originalFetch = globalThis.fetch;
  globalThis.fetch = vi.fn(async (...args) => {
    console.log('fetch called with:', args);
    return originalFetch(...args);
  });
  globalThis.fetch.__original = originalFetch;
}

describe('patch initialization', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  test('initializes fetch patch', () => {
    initializePatches();
    
    expect(globalThis.fetch).toBeDefined();
    expect(globalThis.fetch.__original).toBeDefined();
    expect(vi.isMockFunction(globalThis.fetch)).toBe(true);
  });

  test('next test has clean globals', () => {
    // globalThis.fetch is restored to original
    expect(vi.isMockFunction(globalThis.fetch)).toBe(false);
  });
});
```

The key to success is **consistent cleanup** and **understanding your testing framework's lifecycle hooks**. Both Jest and Vitest provide excellent tools—choose the approach that matches your testing style and infrastructure.
<span style="display:none">[^1_10][^1_11][^1_12][^1_13][^1_14][^1_15][^1_16][^1_17][^1_18][^1_19][^1_20][^1_21][^1_22][^1_23][^1_24][^1_25][^1_26][^1_27][^1_28][^1_29][^1_30]</span>

<div align="center">⁂</div>

[^1_1]: https://vitest.dev/guide/mocking

[^1_2]: https://stackoverflow.com/questions/40449434/mocking-globals-in-jest

[^1_3]: https://vitest.dev/api/vi

[^1_4]: https://runebook.dev/en/articles/vitest/api/vi/vi-unstuballglobals-0-26-0

[^1_5]: https://testing-library.com/docs/using-fake-timers/

[^1_6]: https://www.browserstack.com/guide/monkey-patching

[^1_7]: https://www.jstwister.com/post/unit-testing-beginners-guide-spying-fake-timers/

[^1_8]: https://mmazzarolo.com/blog/2022-07-30-checking-if-a-javascript-native-function-was-monkey-patched/

[^1_9]: https://docs.pytest.org/en/stable/how-to/monkeypatch.html

[^1_10]: https://playcode.io/javascript/variable-scope

[^1_11]: https://stackoverflow.com/questions/77743427/browser-extension-monkey-patching-fetch-responses-from-the-actual-webpage

[^1_12]: https://www.w3schools.com/js/js_scope.asp

[^1_13]: https://github.com/timmywheels/monkey-fetch

[^1_14]: https://www.youtube.com/watch?v=0vXPEqHCrao

[^1_15]: https://www.reddit.com/r/learnprogramming/comments/vfi9b6/how_do_i_avoid_polluting_the_global_scope_in/

[^1_16]: https://blog.logrocket.com/intercepting-javascript-fetch-api-requests-responses/

[^1_17]: https://blog.logrocket.com/advanced-guide-vitest-testing-mocking/

[^1_18]: https://strapi.io/blog/global-variable-in-javascript

[^1_19]: https://blog.codinghorror.com/monkeypatching-for-humans/

[^1_20]: https://stackoverflow.com/questions/73204908/how-to-test-a-function-that-modifies-the-global-scope

[^1_21]: https://stackoverflow.com/questions/67770314/cleartimeout-test-not-working-with-jest-usefaketimers

[^1_22]: https://stackoverflow.com/questions/77622101/vitest-with-react-testing-library-unexpected-component-rendering-between-tests

[^1_23]: https://www.speakeasy.com/blog/vitest-vs-jest

[^1_24]: https://pytest-with-eric.com/mocking/pytest-monkeypatch/

[^1_25]: https://stackoverflow.com/questions/51126786/jest-fake-timers-with-promises/63296414

[^1_26]: https://vitest.dev/guide/migration.html

[^1_27]: https://github.com/jsdom/jsdom/issues/2375

[^1_28]: https://vitest.dev/api/

[^1_29]: https://stackoverflow.com/questions/5741877/is-monkey-patching-really-that-bad

[^1_30]: https://www.dhiwise.com/post/leveraging-jest-clearallmocks-for-efficient-testing

