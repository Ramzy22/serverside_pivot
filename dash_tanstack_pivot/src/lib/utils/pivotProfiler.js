const MAX_HISTORY = 500;
const MAX_MEASURE_HISTORY = 500;
const MAX_RENDER_HISTORY = 500;

const now = () => Date.now();

const readLocalStorageFlag = (key) => {
    if (typeof window === 'undefined') return false;
    try {
        return window.localStorage.getItem(key) === '1';
    } catch (error) {
        return false;
    }
};

export const isPivotProfilingEnabled = () => {
    if (typeof window === 'undefined') return false;
    return window.__PIVOT_PROFILE__ === true || readLocalStorageFlag('pivot-profile');
};

const isPivotProfilerConsoleEnabled = () => {
    if (typeof window === 'undefined') return false;
    return window.__PIVOT_PROFILE_CONSOLE__ === true || readLocalStorageFlag('pivot-profile-console');
};

const cloneEntry = (entry) => {
    if (!entry || typeof entry !== 'object') return null;
    return {
        ...entry,
        profile: entry.profile && typeof entry.profile === 'object'
            ? JSON.parse(JSON.stringify(entry.profile))
            : null,
        meta: entry.meta && typeof entry.meta === 'object'
            ? { ...entry.meta }
            : {},
        derived: entry.derived && typeof entry.derived === 'object'
            ? { ...entry.derived }
            : null,
    };
};

const mergePlainObject = (left, right) => {
    const base = left && typeof left === 'object' ? left : {};
    const next = right && typeof right === 'object' ? right : {};
    return { ...base, ...next };
};

const deriveTimings = (entry) => {
    if (!entry || typeof entry !== 'object') return null;
    const queuedAt = Number.isFinite(entry.queuedAt) ? entry.queuedAt : null;
    const emittedAt = Number.isFinite(entry.emittedAt) ? entry.emittedAt : null;
    const responseReceivedAt = Number.isFinite(entry.responseReceivedAt) ? entry.responseReceivedAt : null;
    const committedAt = Number.isFinite(entry.committedAt) ? entry.committedAt : null;
    const finishedAt = Number.isFinite(entry.finishedAt)
        ? entry.finishedAt
        : (committedAt ?? responseReceivedAt ?? emittedAt ?? queuedAt);
    const startedAt = queuedAt ?? emittedAt ?? responseReceivedAt ?? finishedAt;

    return {
        queueMs: queuedAt !== null && emittedAt !== null ? roundMs(emittedAt - queuedAt) : null,
        responseMs: emittedAt !== null && responseReceivedAt !== null ? roundMs(responseReceivedAt - emittedAt) : null,
        renderMs: responseReceivedAt !== null && committedAt !== null ? roundMs(committedAt - responseReceivedAt) : null,
        totalMs: startedAt !== null && finishedAt !== null ? roundMs(finishedAt - startedAt) : null,
        callbackMs: entry.profile?.callback?.totalMs ?? null,
        serviceMs: entry.profile?.service?.totalMs ?? null,
    };
};

const roundMs = (value) => (
    Number.isFinite(Number(value)) ? Math.round(Number(value) * 1000) / 1000 : null
);

const logSummary = (entry) => {
    if (!isPivotProfilerConsoleEnabled() || !entry) return;
    const derived = entry.derived || {};
    // Keep console output compact so repeated scrolls remain readable.
    console.log('[pivot-profiler]', {
        componentId: entry.componentId,
        requestId: entry.requestId,
        kind: entry.kind,
        status: entry.status,
        totalMs: derived.totalMs,
        queueMs: derived.queueMs,
        responseMs: derived.responseMs,
        renderMs: derived.renderMs,
        callbackMs: derived.callbackMs,
        serviceMs: derived.serviceMs,
        stateEpoch: entry.profile?.request?.stateEpoch ?? entry.meta?.stateEpoch ?? null,
        abortGeneration: entry.profile?.request?.abortGeneration ?? entry.meta?.abortGeneration ?? null,
        lifecycleLane: entry.profile?.request?.lifecycleLane ?? entry.meta?.lifecycleLane ?? null,
        cacheKey: entry.profile?.request?.cacheKey ?? entry.profile?.adapter?.responseCacheKey ?? entry.meta?.cacheKey ?? null,
        cancellationOutcome: entry.profile?.request?.cancellationOutcome ?? entry.meta?.cancellationOutcome ?? null,
    });
};

const createPivotProfiler = () => {
    const active = new Map();
    const history = [];
    const measures = [];
    const renderCounts = new Map();
    const renderHistory = [];

    const ensureEntry = (requestId, meta = {}) => {
        if (!requestId) return null;
        const key = String(requestId);
        const existing = active.get(key);
        if (existing) {
            existing.meta = mergePlainObject(existing.meta, meta.meta);
            if (meta.profile && typeof meta.profile === 'object') {
                existing.profile = mergePlainObject(existing.profile, meta.profile);
            }
            Object.entries(meta).forEach(([field, value]) => {
                if (value === undefined || value === null || field === 'meta' || field === 'profile') return;
                existing[field] = value;
            });
            return existing;
        }

        const next = {
            requestId: key,
            componentId: meta.componentId || null,
            kind: meta.kind || 'data',
            status: meta.status || null,
            emittedAt: meta.emittedAt ?? null,
            queuedAt: meta.queuedAt ?? null,
            responseReceivedAt: meta.responseReceivedAt ?? null,
            committedAt: meta.committedAt ?? null,
            finishedAt: meta.finishedAt ?? null,
            profile: meta.profile && typeof meta.profile === 'object' ? { ...meta.profile } : null,
            meta: meta.meta && typeof meta.meta === 'object' ? { ...meta.meta } : {},
        };
        active.set(key, next);
        return next;
    };

    const finalizeEntry = (entry, meta = {}) => {
        if (!entry) return null;
        Object.entries(meta).forEach(([field, value]) => {
            if (value === undefined || value === null || field === 'meta' || field === 'profile') return;
            entry[field] = value;
        });
        if (meta.meta && typeof meta.meta === 'object') {
            entry.meta = mergePlainObject(entry.meta, meta.meta);
        }
        if (meta.profile && typeof meta.profile === 'object') {
            entry.profile = mergePlainObject(entry.profile, meta.profile);
        }
        entry.finishedAt = Number.isFinite(meta.finishedAt) ? meta.finishedAt : (entry.finishedAt ?? now());
        entry.derived = deriveTimings(entry);
        active.delete(entry.requestId);
        const snapshot = cloneEntry(entry);
        history.push(snapshot);
        if (history.length > MAX_HISTORY) {
            history.splice(0, history.length - MAX_HISTORY);
        }
        logSummary(snapshot);
        return snapshot;
    };

    return {
        queue(meta = {}) {
            return ensureEntry(meta.requestId, {
                ...meta,
                queuedAt: meta.queuedAt ?? now(),
            });
        },
        begin(meta = {}) {
            return ensureEntry(meta.requestId, {
                ...meta,
                emittedAt: meta.emittedAt ?? now(),
            });
        },
        response(meta = {}) {
            const entry = ensureEntry(meta.requestId, {
                ...meta,
                responseReceivedAt: meta.responseReceivedAt ?? now(),
            });
            return entry;
        },
        commit(meta = {}) {
            const entry = ensureEntry(meta.requestId, {
                ...meta,
                committedAt: meta.committedAt ?? now(),
            });
            return finalizeEntry(entry, {
                ...meta,
                committedAt: meta.committedAt ?? now(),
                finishedAt: meta.finishedAt ?? meta.committedAt ?? now(),
            });
        },
        resolve(meta = {}) {
            const entry = ensureEntry(meta.requestId, meta);
            return finalizeEntry(entry, {
                ...meta,
                finishedAt: meta.finishedAt ?? now(),
            });
        },
        measure(meta = {}) {
            if (!meta || !meta.name) return null;
            const finishedAt = Number.isFinite(meta.finishedAt) ? meta.finishedAt : now();
            const startedAt = Number.isFinite(meta.startedAt) ? meta.startedAt : null;
            const durationMs = Number.isFinite(meta.durationMs)
                ? roundMs(meta.durationMs)
                : (startedAt !== null ? roundMs(finishedAt - startedAt) : null);
            const entry = {
                name: String(meta.name),
                componentId: meta.componentId || null,
                startedAt,
                finishedAt,
                durationMs,
                meta: meta.meta && typeof meta.meta === 'object' ? { ...meta.meta } : {},
            };
            measures.push(entry);
            if (measures.length > MAX_MEASURE_HISTORY) {
                measures.splice(0, measures.length - MAX_MEASURE_HISTORY);
            }
            if (isPivotProfilerConsoleEnabled()) {
                console.log('[pivot-profiler:measure]', entry);
            }
            return entry;
        },
        render(meta = {}) {
            const componentName = meta.componentName || meta.componentId;
            if (!componentName) return null;
            const key = String(componentName);
            const count = (renderCounts.get(key) || 0) + 1;
            renderCounts.set(key, count);
            const entry = {
                componentName: key,
                componentId: meta.componentId || null,
                count,
                renderedAt: Number.isFinite(meta.renderedAt) ? meta.renderedAt : now(),
                meta: meta.meta && typeof meta.meta === 'object' ? { ...meta.meta } : {},
            };
            renderHistory.push(entry);
            if (renderHistory.length > MAX_RENDER_HISTORY) {
                renderHistory.splice(0, renderHistory.length - MAX_RENDER_HISTORY);
            }
            if (isPivotProfilerConsoleEnabled()) {
                console.log('[pivot-profiler:render]', entry);
            }
            return entry;
        },
        latest(componentId = null) {
            const items = componentId
                ? history.filter((entry) => entry.componentId === componentId)
                : history;
            return items.length > 0 ? items[items.length - 1] : null;
        },
        getHistory(componentId = null) {
            return componentId
                ? history.filter((entry) => entry.componentId === componentId)
                : [...history];
        },
        getMeasures(componentId = null) {
            return componentId
                ? measures.filter((entry) => entry.componentId === componentId)
                : [...measures];
        },
        getRenderCounts() {
            return Object.fromEntries(renderCounts.entries());
        },
        getRenderHistory(componentId = null) {
            return componentId
                ? renderHistory.filter((entry) => entry.componentId === componentId)
                : [...renderHistory];
        },
        clear(componentId = null) {
            if (!componentId) {
                active.clear();
                history.splice(0, history.length);
                measures.splice(0, measures.length);
                renderCounts.clear();
                renderHistory.splice(0, renderHistory.length);
                return;
            }
            Array.from(active.entries()).forEach(([requestId, entry]) => {
                if (entry.componentId === componentId) {
                    active.delete(requestId);
                }
            });
            for (let index = history.length - 1; index >= 0; index -= 1) {
                if (history[index].componentId === componentId) {
                    history.splice(index, 1);
                }
            }
            for (let index = measures.length - 1; index >= 0; index -= 1) {
                if (measures[index].componentId === componentId) {
                    measures.splice(index, 1);
                }
            }
            for (let index = renderHistory.length - 1; index >= 0; index -= 1) {
                if (renderHistory[index].componentId === componentId) {
                    renderHistory.splice(index, 1);
                }
            }
        },
        summary(componentId = null) {
            const items = this.getHistory(componentId);
            if (items.length === 0) return null;
            const totals = items.reduce((acc, entry) => {
                const derived = entry.derived || {};
                acc.count += 1;
                if (Number.isFinite(derived.totalMs)) acc.totalMs += derived.totalMs;
                if (Number.isFinite(derived.queueMs)) acc.queueMs += derived.queueMs;
                if (Number.isFinite(derived.responseMs)) acc.responseMs += derived.responseMs;
                if (Number.isFinite(derived.renderMs)) acc.renderMs += derived.renderMs;
                return acc;
            }, {
                count: 0,
                totalMs: 0,
                queueMs: 0,
                responseMs: 0,
                renderMs: 0,
            });
            return {
                count: totals.count,
                avgTotalMs: roundMs(totals.totalMs / totals.count),
                avgQueueMs: roundMs(totals.queueMs / totals.count),
                avgResponseMs: roundMs(totals.responseMs / totals.count),
                avgRenderMs: roundMs(totals.renderMs / totals.count),
            };
        },
    };
};

export const getPivotProfiler = () => {
    if (typeof window === 'undefined') return null;
    if (!window.__pivotProfiler || typeof window.__pivotProfiler !== 'object') {
        window.__pivotProfiler = createPivotProfiler();
    }
    return window.__pivotProfiler;
};

export const getPivotPerformanceNow = () => {
    if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
        return performance.now();
    }
    return now();
};

export const recordPivotMeasure = (name, startedAt, meta = {}) => {
    if (!isPivotProfilingEnabled()) return null;
    const profiler = getPivotProfiler();
    if (!profiler || typeof profiler.measure !== 'function') return null;
    const finishedAt = getPivotPerformanceNow();
    return profiler.measure({
        name,
        startedAt,
        finishedAt,
        durationMs: Number.isFinite(startedAt) ? finishedAt - startedAt : null,
        componentId: meta.componentId,
        meta,
    });
};
