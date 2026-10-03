import { AcpRpcError } from './acp-errors'
import type { AcpJsonRpcMessage, AcpPeerHandlers } from './acp-json-rpc-peer'

type OpenRequest = { method: string; controller: AbortController; abandon: () => void }

export class AcpIncomingRequests {
  private readonly open = new Map<string | number | null, OpenRequest>()

  constructor(
    private readonly handler: AcpPeerHandlers['onRequest'],
    private readonly send: (message: AcpJsonRpcMessage) => Promise<void>,
    private readonly onFailure: (error: Error) => void,
    private readonly capacity: number,
    private readonly diagnose: (message: string) => void
  ) {}

  close(error: Error): void {
    for (const request of this.open.values()) {
      request.controller.abort(error)
      request.abandon()
    }
    this.open.clear()
  }

  cancel(exceptMethod?: string): void {
    for (const [id, request] of this.open) {
      if (request.method === exceptMethod) {
        continue
      }
      const error = new AcpRpcError(-32800, 'Request cancelled')
      this.open.delete(id)
      request.controller.abort(error)
      request.abandon()
      void this.sendError(id, error)
    }
  }

  handle(id: string | number | null, method: string, params: unknown): void {
    if (this.open.has(id)) {
      this.diagnose('Ignored duplicate ACP incoming request id')
      return
    }
    if (this.open.size >= this.capacity) {
      void this.sendError(id, new AcpRpcError(-32603, 'ACP incoming request capacity exceeded'))
      return
    }
    const controller = new AbortController()
    let abandon = (): void => {}
    const abandoned = new Promise<never>((_resolve, reject) => {
      abandon = () => reject(controller.signal.reason)
    })
    this.open.set(id, { method, controller, abandon })
    const retire = (): void => {
      if (this.open.get(id)?.controller === controller) {
        this.open.delete(id)
      }
    }
    void Promise.race([
      abandoned,
      Promise.resolve().then(() => {
        if (controller.signal.aborted) {
          return undefined
        }
        if (!this.handler) {
          throw new AcpRpcError(-32601, `Unknown ACP client method: ${method}`)
        }
        return this.handler(method, params, { id, signal: controller.signal })
      })
    ])
      .then(async (result) => {
        if (controller.signal.aborted) {
          return
        }
        // The agent may reuse the id as soon as it reads the response.
        retire()
        await this.send({ jsonrpc: '2.0', id, result: result ?? null })
      })
      .catch(async (error) => {
        if (controller.signal.aborted) {
          return
        }
        retire()
        await this.sendError(
          id,
          error instanceof AcpRpcError
            ? error
            : new AcpRpcError(-32603, error instanceof Error ? error.message : String(error))
        )
      })
      .finally(() => {
        retire()
        controller.abort()
      })
  }

  private async sendError(id: string | number | null, error: AcpRpcError): Promise<void> {
    try {
      await this.send({
        jsonrpc: '2.0',
        id,
        error: { code: error.code, message: error.message, data: error.data }
      })
    } catch (failure) {
      this.onFailure(failure instanceof Error ? failure : new Error(String(failure)))
    }
  }
}
