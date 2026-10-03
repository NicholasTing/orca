import type { Readable, Writable } from 'node:stream'
import { z } from 'zod'
import { MAX_TIMER_DELAY_MS } from '../../shared/timer-delay'
import { AcpAuthRequiredError, AcpRequestTimeoutError, AcpRpcError } from './acp-errors'
import { AcpJsonRpcPeer, type AcpPeerOptions, type AcpRequestContext } from './acp-json-rpc-peer'
import { AcpPermissionRequests, type AcpPermissionHandler } from './acp-permission-requests'
import {
  setupAcpSession,
  type AcpSessionStarted,
  type AcpSessionStartOptions
} from './acp-session-setup'
export type { AcpSessionStarted, AcpSessionStartOptions } from './acp-session-setup'
import {
  ACP_PROTOCOL_VERSION,
  InitializeResponseSchema,
  AuthenticateResponseSchema,
  PromptResponseSchema,
  RequestPermissionRequestSchema,
  SessionNotificationSchema,
  SetSessionModeResponseSchema,
  SetSessionModelResponseSchema,
  SetSessionConfigOptionResponseSchema,
  type InitializeRequest,
  type InitializeResponse,
  type AuthenticateResponse,
  type PromptRequest,
  type PromptResponse,
  type SessionNotification,
  type SetSessionConfigOptionRequest,
  type SetSessionConfigOptionResponse,
  type SetSessionModeResponse,
  type SetSessionModelResponse
} from './generated/acp-protocol.generated'

const updateEnvelopeSchema = z.looseObject({
  sessionId: z.string(),
  update: z.looseObject({ sessionUpdate: z.string() })
})
export type AcpSessionEvent =
  | { kind: 'known'; notification: SessionNotification }
  | { kind: 'unrecognized'; sessionId: string; raw: z.infer<typeof updateEnvelopeSchema> }

export type AcpSessionRuntimeOptions = {
  clientInfo?: InitializeRequest['clientInfo']
  peer?: AcpPeerOptions
  cancelTimeoutMs?: number
  onPermission?: AcpPermissionHandler
  onRequest?: (method: string, params: unknown, context: AcpRequestContext) => unknown
  onDiagnostic?: (message: string) => void
  onClose?: (error: Error) => void
}
type ActivePrompt = {
  response: Promise<PromptResponse>
  cancelling: boolean
  cancelPromise?: Promise<void>
}

export class AcpSessionRuntime {
  private readonly peer: AcpJsonRpcPeer
  private readonly permissions = new AcpPermissionRequests()
  private readonly listeners = new Set<(event: AcpSessionEvent) => void>()
  private initialized?: Promise<InitializeResponse>
  private starting?: Promise<AcpSessionStarted>
  private started?: AcpSessionStarted
  private activePrompt?: ActivePrompt
  private reportedUpdateAnomaly = false

  constructor(
    input: Readable,
    output: Writable,
    private readonly options: AcpSessionRuntimeOptions = {}
  ) {
    const timeout = options.cancelTimeoutMs
    if (timeout !== undefined && (!Number.isSafeInteger(timeout) || timeout <= 0)) {
      throw new Error('ACP timeouts must be positive finite timer durations')
    }
    this.peer = new AcpJsonRpcPeer(
      input,
      output,
      {
        onRequest: (method, params, context) => this.handleRequest(method, params, context),
        onNotification: (method, params) => this.handleNotification(method, params),
        onDiagnostic: options.onDiagnostic,
        onClose: options.onClose
      },
      options.peer
    )
  }

  subscribe(listener: (event: AcpSessionEvent) => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  initialize(): Promise<InitializeResponse> {
    this.initialized ??= this.call(
      'initialize',
      {
        protocolVersion: ACP_PROTOCOL_VERSION,
        clientCapabilities: { fs: { readTextFile: false, writeTextFile: false }, terminal: false },
        ...(this.options.clientInfo === undefined ? {} : { clientInfo: this.options.clientInfo })
      } satisfies InitializeRequest,
      InitializeResponseSchema
    )
      .then((response) => {
        if (response.protocolVersion !== ACP_PROTOCOL_VERSION) {
          const error = new AcpRpcError(
            -32602,
            `Unsupported ACP protocol version: ${response.protocolVersion}`
          )
          this.peer.close(error)
          throw error
        }
        return response
      })
      .catch((error) => {
        this.initialized = undefined
        throw error
      })
    return this.initialized
  }

  async authenticate(methodId: string): Promise<AuthenticateResponse> {
    await this.initialize()
    return this.call('authenticate', { methodId }, AuthenticateResponseSchema)
  }

  start(options: AcpSessionStartOptions): Promise<AcpSessionStarted> {
    if (this.started) {
      return Promise.resolve(this.started)
    }
    this.starting ??= this.initialize()
      .then((initialized) =>
        setupAcpSession(initialized, options, (method, params, schema) =>
          this.call(method, params, schema)
        )
      )
      .then((started) => {
        this.started = started
        return started
      })
      .catch((error) => {
        this.starting = undefined
        throw error
      })
    return this.starting
  }

  async prompt(prompt: PromptRequest['prompt']): Promise<PromptResponse> {
    if (this.activePrompt) {
      throw new Error('ACP prompt already in progress')
    }
    const sessionId = this.sessionId()
    const active: ActivePrompt = {
      cancelling: false,
      response: this.call('session/prompt', { sessionId, prompt }, PromptResponseSchema)
    }
    this.activePrompt = active
    active.response = active.response.finally(() => {
      if (this.activePrompt === active) {
        this.activePrompt = undefined
      }
    })
    return active.response
  }

  cancel(): Promise<void> {
    if (!this.started) {
      return Promise.reject(new Error('ACP session has not started'))
    }
    const sessionId = this.started.sessionId
    this.permissions.cancel(sessionId)
    this.peer.cancelIncomingRequests('session/request_permission')
    const active = this.activePrompt
    if (!active) {
      return Promise.resolve()
    }
    active.cancelling = true
    active.cancelPromise ??= this.cancelActive(sessionId, active)
    return active.cancelPromise
  }

  private async cancelActive(sessionId: string, active: ActivePrompt): Promise<void> {
    const timeoutMs = Math.min(this.options.cancelTimeoutMs ?? 10_000, MAX_TIMER_DELAY_MS)
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      await Promise.race([
        this.peer.notify('session/cancel', { sessionId }).then(() =>
          active.response.catch((error) => {
            if (this.peer.closed) {
              throw error
            }
          })
        ),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => {
            const error = new AcpRequestTimeoutError('session/cancel')
            this.peer.close(error)
            reject(error)
          }, timeoutMs)
        })
      ])
    } finally {
      clearTimeout(timer)
    }
  }

  async setMode(modeId: string): Promise<SetSessionModeResponse> {
    return this.call(
      'session/set_mode',
      { sessionId: this.sessionId(), modeId },
      SetSessionModeResponseSchema
    )
  }
  async setModel(modelId: string): Promise<SetSessionModelResponse> {
    return this.call(
      'session/set_model',
      { sessionId: this.sessionId(), modelId },
      SetSessionModelResponseSchema
    )
  }
  async setConfigOption(
    configId: SetSessionConfigOptionRequest['configId'],
    value: SetSessionConfigOptionRequest['value']
  ): Promise<SetSessionConfigOptionResponse> {
    const sessionId = this.sessionId()
    const request =
      typeof value === 'boolean'
        ? ({ configId, value, sessionId, type: 'boolean' } satisfies SetSessionConfigOptionRequest)
        : ({ configId, value, sessionId } satisfies SetSessionConfigOptionRequest)
    return this.call('session/set_config_option', request, SetSessionConfigOptionResponseSchema)
  }

  // The process owner must call close on child exit, even if descendants keep stdio open.
  close(error?: Error): void {
    this.peer.close(error)
    this.listeners.clear()
  }

  private sessionId(): string {
    if (!this.started) {
      throw new Error('ACP session has not started')
    }
    return this.started.sessionId
  }

  private async call<T>(method: string, params: unknown, schema: z.ZodType<T>): Promise<T> {
    const result = await this.peer.request(method, params, { timeoutMs: null }).catch((error) => {
      if (error instanceof AcpRpcError && error.code === -32000) {
        throw new AcpAuthRequiredError(error.message, error.data)
      }
      throw error
    })
    const parsed = schema.safeParse(result)
    if (!parsed.success) {
      throw new AcpRpcError(-32603, `Invalid ACP response: ${method}`)
    }
    return parsed.data
  }

  private handleRequest(method: string, params: unknown, context: AcpRequestContext): unknown {
    if (method !== 'session/request_permission') {
      if (!this.options.onRequest) {
        throw new AcpRpcError(-32601, `Unknown ACP client method: ${method}`)
      }
      return this.options.onRequest(method, params, context)
    }
    const parsed = RequestPermissionRequestSchema.safeParse(params)
    if (!parsed.success) {
      throw new AcpRpcError(-32602, 'Invalid ACP permission request')
    }
    if (
      !this.activePrompt ||
      this.activePrompt.cancelling ||
      parsed.data.sessionId !== this.started?.sessionId
    ) {
      return { outcome: { outcome: 'cancelled' } }
    }
    return this.permissions.handle(parsed.data, context, this.options.onPermission)
  }

  private diagnose(message: string): void {
    try {
      this.options.onDiagnostic?.(message)
    } catch {
      /* Diagnostics cannot prevent event delivery. */
    }
  }

  private handleNotification(method: string, params: unknown): void {
    if (method !== 'session/update') {
      return
    }
    const envelope = updateEnvelopeSchema.safeParse(params)
    if (!envelope.success) {
      this.diagnose('Ignored invalid ACP session update envelope')
      return
    }
    const parsed = SessionNotificationSchema.safeParse(params)
    const event: AcpSessionEvent = parsed.success
      ? { kind: 'known', notification: parsed.data }
      : { kind: 'unrecognized', sessionId: envelope.data.sessionId, raw: envelope.data }
    if (!parsed.success && !this.reportedUpdateAnomaly) {
      this.reportedUpdateAnomaly = true
      this.diagnose('Forwarded unrecognized ACP session update')
    }
    for (const listener of this.listeners) {
      try {
        listener(event)
      } catch (error) {
        this.diagnose(`ACP event listener failed: ${String(error)}`)
      }
    }
  }
}
