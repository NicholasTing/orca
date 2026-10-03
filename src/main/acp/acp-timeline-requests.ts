import { AcpRpcError } from './acp-errors'
import type { AcpRequestPresentation } from './acp-dialects/acp-dialect'
import { RequestPermissionRequestSchema } from './generated/acp-protocol.generated'

export const pendingAcpResolution = {
  state: 'pending',
  selectedOptionId: null,
  resolvedBy: null,
  resolvedAt: null
} as const

export function acpPermissionPresentation(params: unknown): AcpRequestPresentation {
  const parsed = RequestPermissionRequestSchema.safeParse(params)
  if (!parsed.success) {
    throw new AcpRpcError(-32602, 'Invalid ACP permission request')
  }
  const { toolCall, options } = parsed.data
  return {
    body: {
      kind: 'approval',
      title: toolCall.title ?? 'Permission requested',
      detail: null,
      options: options.map((option) => ({ id: option.optionId, label: option.name })),
      resolution: pendingAcpResolution
    },
    reply: (response) => {
      if (response === null) {
        return { outcome: { outcome: 'cancelled' } }
      }
      if (
        response.kind !== 'option' ||
        !options.some((option) => option.optionId === response.optionId)
      ) {
        throw new AcpRpcError(-32602, 'Permission answer must select an offered option')
      }
      return { outcome: { outcome: 'selected', optionId: response.optionId } }
    }
  }
}
