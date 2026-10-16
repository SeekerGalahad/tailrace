/**
 * Artifact API (design doc §4.4). Upload is multipart with a relaxed 300s
 * timeout; `name+version` duplicates → 409; referenced artifacts → 409 on delete.
 */

import { http, type HttpOptions } from './http'

export interface Artifact {
  id: number
  name: string
  version: string
  sizeBytes: number
  /** Deploy verification anchor (sha256, server-computed). */
  sha256: string
  refCount: number
  source: string
  uploadedBy: string
  createdAt: string
}

export interface ArtifactList {
  artifacts: Artifact[]
}

export interface UploadArtifactInput {
  /** .tar.gz bundle: tailrace-node + nodectl.sh + VERSION (§4.4). */
  file: File
  /** Defaults to "tailrace-node"; charset [A-Za-z0-9._-] only. */
  name?: string
  version: string
}

export interface UploadArtifactResponse {
  id: number
  sha256: string
}

export const artifactsApi = {
  /** GET /api/artifacts */
  list(options?: HttpOptions): Promise<ArtifactList> {
    return http.get('/api/artifacts', options)
  },

  /** POST /api/artifacts (multipart) — 201 {id, sha256}; timeout relaxed to 300s. */
  upload(input: UploadArtifactInput, options?: HttpOptions): Promise<UploadArtifactResponse> {
    const form = new FormData()
    form.set('file', input.file)
    form.set('name', input.name ?? 'tailrace-node')
    form.set('version', input.version)
    return http.post('/api/artifacts', form, { timeoutMs: 300_000, ...options })
  },

  /** DELETE /api/artifacts/{id} — 409 while any node references it. */
  remove(id: number | string, options?: HttpOptions): Promise<void> {
    return http.delete(`/api/artifacts/${id}`, options)
  },
}
