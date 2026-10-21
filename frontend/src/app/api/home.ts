export type HomeOverview = {
  captured_at: string;
  connections?: { own: number; external: number };
  sources?: {
    own_enabled: number;
    own_disabled: number;
    own_archived: number;
    external_enabled: number;
    external_disabled: number;
  };
  agents?: {
    provisioning: number;
    ready: number;
    error: number;
    revocation_pending: number;
    revoked: number;
  };
  normalizers?: number;
};
