import {
    type JsonObject,
    type RuntimeRequestV1,
    type RuntimeResponseV1,
    isRuntimeResponseV1,
} from './runtimeContract';

export interface PayloadReference {
    readonly id: string;
    readonly url: string;
    readonly bytes?: number;
    readonly contentType?: string;
    readonly expiresAt?: number;
}

export interface RuntimeTransportRequestOptions {
    readonly signal?: AbortSignal;
}

/** The framework-neutral runtime transport implemented by every host. */
export interface PivotTransport {
    request(
        request: RuntimeRequestV1,
        options?: RuntimeTransportRequestOptions,
    ): Promise<RuntimeResponseV1>;
    fetchPayload(
        reference: PayloadReference,
        options?: RuntimeTransportRequestOptions,
    ): Promise<JsonObject>;
    /** Binary completed-export delivery, retaining the transport's auth context. */
    fetchExportPayload?(
        reference: PayloadReference,
        options?: RuntimeTransportRequestOptions,
    ): Promise<Blob>;
}

export type RuntimePayloadTransport = Pick<PivotTransport, 'fetchPayload' | 'fetchExportPayload'>;

export interface HttpPivotTransportOptions {
    readonly endpoint?: string;
    readonly fetchImpl?: typeof fetch;
    readonly headers?: Readonly<Record<string, string>>;
}

const abortError = (): Error => {
    const error = new Error('Superseded viewport request.');
    error.name = 'AbortError';
    return error;
};

const readJson = async (response: Response): Promise<unknown> => {
    const contentType = response.headers.get('content-type') || '';
    if (!contentType.includes('application/json')) {
        throw new Error(`Runtime response must be JSON; received ${contentType || 'unknown content type'}.`);
    }
    return response.json();
};

/**
 * Direct HTTP transport for React, Streamlit, and any non-Dash host.
 *
 * It transfers compact V1 envelopes only. Oversized row blocks are fetched
 * through `fetchPayload`, where the browser can cache and abort independently.
 */
export class HttpPivotTransport implements PivotTransport {
    private readonly endpoint: string;
    private readonly fetchImpl: typeof fetch;
    private readonly headers: Readonly<Record<string, string>>;

    constructor(options: HttpPivotTransportOptions = {}) {
        this.endpoint = options.endpoint || '/v1/runtime';
        // Streamlit V2 executes the shared React workspace in an iframe whose
        // Window implementation requires `fetch` to retain its receiver.
        // Calling a captured bare function raises "Illegal invocation" before
        // the request reaches the host-neutral runtime. Preserve callers'
        // injected implementation unchanged, but bind the browser default to
        // its owning global for React, Streamlit, and embedded web views.
        this.fetchImpl = options.fetchImpl || globalThis.fetch.bind(globalThis);
        this.headers = options.headers || {};
    }

    /**
     * Runtime gateways intentionally return path-relative payload references
     * so same-origin deployments remain portable. Resolve them against the
     * configured runtime endpoint here: an embedded Streamlit host may live
     * on a different origin and must never turn ``/v1/runtime/payload/...``
     * into a request to the Streamlit server.
     */
    private resolvePayloadUrl(url: string): string {
        try {
            const pageUrl = typeof window !== 'undefined' ? window.location.href : undefined;
            const endpointUrl = pageUrl ? new URL(this.endpoint, pageUrl).toString() : this.endpoint;
            return new URL(url, endpointUrl).toString();
        } catch {
            // Preserve an opaque/custom URL unchanged. Fetch will provide the
            // useful error if a host supplied an invalid endpoint.
            return url;
        }
    }

    private resolvePayloadReference(response: RuntimeResponseV1): RuntimeResponseV1 {
        const payloadRef = response.payload.payloadRef;
        if (!payloadRef || typeof payloadRef !== 'object' || Array.isArray(payloadRef)) return response;
        const url = payloadRef.url;
        if (typeof url !== 'string' || !url) return response;
        const resolvedUrl = this.resolvePayloadUrl(url);
        if (resolvedUrl === url) return response;
        return {
            ...response,
            payload: {
                ...response.payload,
                payloadRef: { ...payloadRef, url: resolvedUrl },
            },
        };
    }

    async request(
        request: RuntimeRequestV1,
        options: RuntimeTransportRequestOptions = {},
    ): Promise<RuntimeResponseV1> {
        const response = await this.fetchImpl(this.endpoint, {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
                ...this.headers,
            },
            body: JSON.stringify(request),
            signal: options.signal,
        });
        if (!response.ok) {
            // Admission/authentication failures use the same V1 envelope as a
            // successful request. Preserve their safe metadata for consumers;
            // opaque gateways and malformed bodies must not expose raw text.
            try {
                const body = await readJson(response);
                if (isRuntimeResponseV1(body) && body.status === 'error') return body;
            } catch (error) {
                if (error instanceof Error && error.name === 'AbortError') throw error;
            }
            throw new Error(`Runtime request failed with status ${response.status}.`);
        }
        const body = await readJson(response);
        if (!isRuntimeResponseV1(body)) {
            throw new Error('Runtime server returned an invalid V1 response.');
        }
        return this.resolvePayloadReference(body);
    }

    async fetchPayload(
        reference: PayloadReference,
        options: RuntimeTransportRequestOptions = {},
    ): Promise<JsonObject> {
        const response = await this.fetchImpl(this.resolvePayloadUrl(reference.url), {
            method: 'GET',
            credentials: 'same-origin',
            headers: { Accept: 'application/json', ...this.headers },
            signal: options.signal,
        });
        if (!response.ok) {
            throw new Error(`Runtime payload fetch failed with status ${response.status}.`);
        }
        const body = await readJson(response);
        if (!body || typeof body !== 'object' || Array.isArray(body)) {
            throw new Error('Runtime payload must be an object.');
        }
        return body as JsonObject;
    }

    async fetchExportPayload(
        reference: PayloadReference,
        options: RuntimeTransportRequestOptions = {},
    ): Promise<Blob> {
        const response = await this.fetchImpl(this.resolvePayloadUrl(reference.url), {
            method: 'GET', credentials: 'same-origin',
            headers: { Accept: '*/*', ...this.headers }, signal: options.signal,
        });
        if (!response.ok) throw new Error(`Export payload fetch failed with status ${response.status}.`);
        return response.blob();
    }
}

interface QueuedViewportRequest {
    readonly request: RuntimeRequestV1;
    readonly resolve: (response: RuntimeResponseV1) => void;
    readonly reject: (reason: unknown) => void;
}

/**
 * Coalesces a burst of viewport events into the latest request and aborts the
 * in-flight request as soon as a newer window supersedes it. Structural,
 * export, chart, and detail requests bypass this scheduler.
 */
export class LatestViewportScheduler implements PivotTransport {
    private active: AbortController | null = null;
    private queued: QueuedViewportRequest | null = null;
    private flushScheduled = false;

    constructor(private readonly transport: PivotTransport) {}

    request(
        request: RuntimeRequestV1,
        options: RuntimeTransportRequestOptions = {},
    ): Promise<RuntimeResponseV1> {
        const isViewport = request.kind === 'data'
            && (request.payload.intent === 'viewport' || request.payload.intent === 'structural');
        if (!isViewport) return this.transport.request(request, options);

        if (this.active) this.active.abort();
        if (this.queued) this.queued.reject(abortError());
        return new Promise<RuntimeResponseV1>((resolve, reject) => {
            this.queued = { request, resolve, reject };
            if (!this.flushScheduled) {
                this.flushScheduled = true;
                Promise.resolve().then(() => this.flush(options));
            }
        });
    }

    fetchPayload(
        reference: PayloadReference,
        options: RuntimeTransportRequestOptions = {},
    ): Promise<JsonObject> {
        return this.transport.fetchPayload(reference, options);
    }

    fetchExportPayload(
        reference: PayloadReference,
        options: RuntimeTransportRequestOptions = {},
    ): Promise<Blob> {
        return this.transport.fetchExportPayload
            ? this.transport.fetchExportPayload(reference, options)
            : Promise.reject(new Error('The configured transport does not support export payload delivery.'));
    }

    private async flush(options: RuntimeTransportRequestOptions): Promise<void> {
        this.flushScheduled = false;
        const queued = this.queued;
        this.queued = null;
        if (!queued) return;
        const controller = new AbortController();
        this.active = controller;
        const abortFromCaller = () => controller.abort();
        if (options.signal) {
            if (options.signal.aborted) controller.abort();
            else options.signal.addEventListener('abort', abortFromCaller, { once: true });
        }
        try {
            queued.resolve(await this.transport.request(queued.request, { signal: controller.signal }));
        } catch (error) {
            queued.reject(error);
        } finally {
            if (options.signal) options.signal.removeEventListener('abort', abortFromCaller);
            if (this.active === controller) this.active = null;
        }
    }
}
