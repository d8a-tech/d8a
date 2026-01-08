// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Import the duplicator. Since it attaches to window, we just import the file.
import './ga4-duplicator';

describe('GA4 Duplicator Smoke Test', () => {
    const SERVER_URL = 'https://my-d8a-server.com/collect';
    const GA4_URL = 'https://www.google-analytics.com/g/collect?v=2&tid=G-12345&gtm=297&tag_exp=1';

    let fetchMock: any;

    beforeEach(() => {
        // Clear window state if necessary
        delete (window as any).__ga4DuplicatorInitialized;

        // Mock fetch
        fetchMock = vi.fn().mockResolvedValue(new Response('ok'));
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('should duplicate a GA4 fetch request', async () => {
        // given
        (window as any).createGA4Duplicator({
            server_container_url: SERVER_URL,
            debug: true
        });

        // when
        await fetch(GA4_URL, { method: 'GET' });

        // then
        // fetch should have been called twice: 
        // 1. Original GA4 call
        // 2. Duplicate call to D8A server
        expect(fetchMock).toHaveBeenCalledTimes(2);

        const firstCallUrl = fetchMock.mock.calls[0][0];
        const secondCallUrl = fetchMock.mock.calls[1][0];

        expect(firstCallUrl).toBe(GA4_URL);
        expect(secondCallUrl).toContain(SERVER_URL);
        expect(secondCallUrl).toContain('tid=G-12345');
    });

    it('should route to different destinations based on tid', async () => {
        // given
        const DEST1_URL = 'https://dest1.com';
        const DEST2_URL = 'https://dest2.com';
        const DEFAULT_URL = 'https://default.com';

        (window as any).createGA4Duplicator({
            destinations: [
                { measurement_id: 'G-SPECIFIC1', server_container_url: DEST1_URL },
                { measurement_id: 'G-SPECIFIC2', server_container_url: DEST2_URL }
            ],
            server_container_url: DEFAULT_URL,
            debug: true
        });

        const URL1 = 'https://www.google-analytics.com/g/collect?v=2&tid=G-SPECIFIC1&gtm=1&tag_exp=1';
        const URL2 = 'https://www.google-analytics.com/g/collect?v=2&tid=G-SPECIFIC2&gtm=1&tag_exp=1';
        const URL_OTHER = 'https://www.google-analytics.com/g/collect?v=2&tid=G-OTHER&gtm=1&tag_exp=1';

        // when
        await fetch(URL1, { method: 'GET' });
        await fetch(URL2, { method: 'GET' });
        await fetch(URL_OTHER, { method: 'GET' });

        // then
        // Total 6 calls: 3 original + 3 duplicates
        expect(fetchMock).toHaveBeenCalledTimes(6);

        // Check duplicates
        const dup1 = fetchMock.mock.calls[1][0];
        const dup2 = fetchMock.mock.calls[3][0];
        const dup3 = fetchMock.mock.calls[5][0];

        expect(dup1).toContain(DEST1_URL);
        expect(dup1).toContain('tid=G-SPECIFIC1');

        expect(dup2).toContain(DEST2_URL);
        expect(dup2).toContain('tid=G-SPECIFIC2');

        expect(dup3).toContain(DEFAULT_URL);
        expect(dup3).toContain('tid=G-OTHER');
    });
});
