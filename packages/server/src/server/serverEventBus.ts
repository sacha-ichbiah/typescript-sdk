import type { JSONRPCNotification, ServerCapabilities, SubscriptionFilter } from '@modelcontextprotocol/core-internal';

/**
 * A change event a server publishes for delivery on open `subscriptions/listen`
 * streams. Each variant maps onto exactly one notification method:
 *
 * - `tools_list_changed` → `notifications/tools/list_changed`
 * - `prompts_list_changed` → `notifications/prompts/list_changed`
 * - `resources_list_changed` → `notifications/resources/list_changed`
 * - `resource_updated` → `notifications/resources/updated` (carries the URI)
 * - `extension` → `method`, a notification owned by a {@linkcode ListenExtension}
 *
 * The bus carries the event and any forwarded notification params — the
 * entry's listen router owns subscription-id stamping and per-stream filtering.
 */
export type ServerEvent = (
    | { kind: 'tools_list_changed' }
    | { kind: 'prompts_list_changed' }
    | { kind: 'resources_list_changed' }
    | { kind: 'resource_updated'; uri: string }
    | { kind: 'extension'; method: string }
) & {
    /** Forwarded notification params; the router stamps its own subscription ID and the event's resource URI. */
    params?: JSONRPCNotification['params'];
};

/**
 * The server-side change-event seam for `subscriptions/listen`.
 *
 * The serving entry (`createMcpHandler`) owns the per-stream listen router:
 * each open `subscriptions/listen` stream registers a listener via
 * `subscribe()`, and consumer code (typically via `handler.notify.*` sugar)
 * publishes change events via `publish()`. In-process servers can use the
 * default {@linkcode InMemoryServerEventBus}; multi-process deployments
 * implement this interface over their own pub/sub.
 *
 * The SDK owns wire semantics (ack-first, filtering, subscription-id
 * stamping, teardown); a `ServerEventBus` only sources the events. It MUST
 * NOT echo back to the listener that published an event when called from
 * inside that listener (no surprise here — the default delivers
 * synchronously and listeners never publish).
 */
export interface ServerEventBus {
    /**
     * Publish a change event to every registered listener.
     */
    publish(event: ServerEvent): void;
    /**
     * Register a listener; returns an idempotent unsubscribe function.
     */
    subscribe(listener: (event: ServerEvent) => void): () => void;
}

/**
 * A `ServerEventBus` backed by an in-process listener set.
 *
 * `publish()` delivers synchronously to the live listener set (a listener
 * unsubscribing itself mid-dispatch is safe; the entry's listen-router
 * listeners never unsubscribe peers). A throwing listener does not stop
 * delivery to the others.
 */
export class InMemoryServerEventBus implements ServerEventBus {
    private readonly _listeners = new Set<(event: ServerEvent) => void>();

    /**
     * @param onerror - Optional callback for errors thrown by listeners
     *   during dispatch.
     */
    constructor(private readonly onerror?: (error: Error) => void) {}

    publish(event: ServerEvent): void {
        for (const listener of this._listeners) {
            try {
                listener(event);
            } catch (error) {
                this.onerror?.(error instanceof Error ? error : new Error(String(error)));
            }
        }
    }

    subscribe(listener: (event: ServerEvent) => void): () => void {
        this._listeners.add(listener);
        let live = true;
        return () => {
            if (!live) return;
            live = false;
            this._listeners.delete(listener);
        };
    }

    /** The number of currently registered listeners (test/introspection only — the routers track capacity via their own open-subscription set). */
    get listenerCount(): number {
        return this._listeners.size;
    }
}

/**
 * Typed publish-side facade over `bus.publish` returned by `createMcpHandler`:
 * each method publishes the corresponding {@linkcode ServerEvent}. Prefer this
 * over calling `bus.publish` directly — the names match the wire methods.
 */
export interface ServerNotifier {
    /** Publish `notifications/tools/list_changed` to every open subscription that opted in. */
    toolsChanged(): void;
    /** Publish `notifications/prompts/list_changed` to every open subscription that opted in. */
    promptsChanged(): void;
    /** Publish `notifications/resources/list_changed` to every open subscription that opted in. */
    resourcesChanged(): void;
    /** Publish `notifications/resources/updated` for `uri` to every open subscription that opted in to that URI. */
    resourceUpdated(uri: string): void;
}

/** Build a {@linkcode ServerNotifier} over a bus. */
export function createServerNotifier(bus: ServerEventBus): ServerNotifier {
    return {
        toolsChanged: () => bus.publish({ kind: 'tools_list_changed' }),
        promptsChanged: () => bus.publish({ kind: 'prompts_list_changed' }),
        resourcesChanged: () => bus.publish({ kind: 'resources_list_changed' }),
        resourceUpdated: (uri: string) => bus.publish({ kind: 'resource_updated', uri })
    };
}

/**
 * Whether a `subscriptions/listen` filter accepts a given change event.
 *
 * Pure: no I/O, no mutation. The filter governs ONLY the four
 * subscription-gated change types — non-gated notifications never reach the
 * bus and are not modeled here.
 *
 * `resource_updated` matches only when `resourceSubscriptions` is present and
 * contains the event's URI exactly (per the spec: "for these resource URIs").
 */
export function listenFilterAccepts(filter: SubscriptionFilter, event: ServerEvent): boolean {
    switch (event.kind) {
        case 'tools_list_changed': {
            return filter.toolsListChanged === true;
        }
        case 'prompts_list_changed': {
            return filter.promptsListChanged === true;
        }
        case 'resources_list_changed': {
            return filter.resourcesListChanged === true;
        }
        case 'resource_updated': {
            return filter.resourceSubscriptions !== undefined && filter.resourceSubscriptions.includes(event.uri);
        }
        case 'extension': {
            return false;
        }
    }
}

/**
 * An extension's part in `subscriptions/listen`: the notification methods it
 * owns and the filter fields that opt a stream in to them. Pass it to
 * `createMcpHandler` or `serveStdio` as `listenExtensions`.
 *
 * Its fields sit next to the spec's own in the request's `notifications`
 * filter. The acknowledgement carries what {@linkcode ListenExtension.honor}
 * returns, and a notification it owns reaches a stream only when
 * {@linkcode ListenExtension.accepts} says so.
 */
export interface ListenExtension {
    /** The notification methods this extension owns, for example `notifications/tasks`. */
    methods: readonly string[];
    /**
     * The extension's fields of the requested filter that this server honors,
     * given its capabilities, or `undefined` to honor none.
     */
    honor(requested: Readonly<Record<string, unknown>>, capabilities: ServerCapabilities): Record<string, unknown> | undefined;
    /** Whether one of the extension's notifications reaches a stream with this acknowledged filter. */
    accepts(honored: Readonly<Record<string, unknown>>, notification: { method: string; params?: JSONRPCNotification['params'] }): boolean;
}

const CORE_FILTER_FIELDS: ReadonlySet<string> = new Set([
    'toolsListChanged',
    'promptsListChanged',
    'resourcesListChanged',
    'resourceSubscriptions'
]);

/** The core notification methods `subscriptions/listen` filters itself; no extension may own one. */
export const CORE_CHANGE_NOTIFICATION_METHODS: ReadonlySet<string> = new Set([
    'notifications/tools/list_changed',
    'notifications/prompts/list_changed',
    'notifications/resources/list_changed',
    'notifications/resources/updated'
]);

/** Throws when two extensions own the same method, or one owns a core change notification. */
export function assertListenExtensions(extensions: readonly ListenExtension[]): void {
    const owned = new Set<string>();
    for (const extension of extensions) {
        for (const method of extension.methods) {
            if (CORE_CHANGE_NOTIFICATION_METHODS.has(method) || owned.has(method)) {
                throw new Error(`listenExtensions: ${method} is already owned`);
            }
            owned.add(method);
        }
    }
}

/**
 * The acknowledged filter: the honored core subset plus every extension's
 * honored fields. An extension cannot honor a core field.
 */
export function honoredListenFilter(
    requested: Readonly<Record<string, unknown>>,
    core: SubscriptionFilter,
    capabilities: ServerCapabilities,
    extensions: readonly ListenExtension[]
): SubscriptionFilter & Record<string, unknown> {
    const honored: SubscriptionFilter & Record<string, unknown> = { ...honoredSubset(core, capabilities) };
    for (const extension of extensions) {
        for (const [key, value] of Object.entries(extension.honor(requested, capabilities) ?? {})) {
            if (!CORE_FILTER_FIELDS.has(key)) honored[key] = value;
        }
    }
    return honored;
}

/** Whether a stream with this acknowledged filter receives the event, core or extension. */
export function listenAccepts(
    honored: SubscriptionFilter & Readonly<Record<string, unknown>>,
    event: ServerEvent,
    extensions: readonly ListenExtension[]
): boolean {
    if (event.kind !== 'extension') return listenFilterAccepts(honored, event);
    const owner = extensions.find(extension => extension.methods.includes(event.method));
    return (
        owner !== undefined && owner.accepts(honored, { method: event.method, ...(event.params !== undefined && { params: event.params }) })
    );
}

/**
 * The honored subset of a requested filter: keeps only the fields the client
 * explicitly opted in to (drops `false` and absent fields), narrowed against
 * the server's declared capabilities when supplied. The serving entry sends
 * this back in `notifications/subscriptions/acknowledged` so the ack reflects
 * what the server can actually deliver.
 *
 * - `toolsListChanged` is honored only when `capabilities.tools.listChanged`
 *   is advertised; likewise `promptsListChanged` / `resourcesListChanged`.
 * - `resourceSubscriptions` is honored only when
 *   `capabilities.resources.subscribe` is advertised.
 *
 * `capabilities` is optional on this pure helper for test convenience only —
 * both wired routers REQUIRE capabilities at the call site (the HTTP router's
 * `serve()` takes a required parameter; `StdioListenRouter.serve()` throws
 * before `setServerCapabilities()` was called), so the fail-open
 * `undefined → honor everything` branch is never reachable on a wired entry.
 */
export function honoredSubset(requested: SubscriptionFilter, capabilities?: ServerCapabilities): SubscriptionFilter {
    const honored: SubscriptionFilter = {};
    const allow = (bit: unknown): boolean => capabilities === undefined || bit === true;
    if (requested.toolsListChanged === true && allow(capabilities?.tools?.listChanged)) honored.toolsListChanged = true;
    if (requested.promptsListChanged === true && allow(capabilities?.prompts?.listChanged)) honored.promptsListChanged = true;
    if (requested.resourcesListChanged === true && allow(capabilities?.resources?.listChanged)) honored.resourcesListChanged = true;
    if (
        requested.resourceSubscriptions !== undefined &&
        requested.resourceSubscriptions.length > 0 &&
        allow(capabilities?.resources?.subscribe)
    ) {
        honored.resourceSubscriptions = [...requested.resourceSubscriptions];
    }
    return honored;
}

/** Map a {@linkcode ServerEvent} onto its wire notification `{method, params}`. */
export function serverEventToNotification(event: ServerEvent): { method: string; params?: JSONRPCNotification['params'] } {
    switch (event.kind) {
        case 'tools_list_changed': {
            return { method: 'notifications/tools/list_changed', ...(event.params !== undefined && { params: event.params }) };
        }
        case 'prompts_list_changed': {
            return { method: 'notifications/prompts/list_changed', ...(event.params !== undefined && { params: event.params }) };
        }
        case 'resources_list_changed': {
            return { method: 'notifications/resources/list_changed', ...(event.params !== undefined && { params: event.params }) };
        }
        case 'resource_updated': {
            return { method: 'notifications/resources/updated', params: { ...event.params, uri: event.uri } };
        }
        case 'extension': {
            return { method: event.method, ...(event.params !== undefined && { params: event.params }) };
        }
    }
}
