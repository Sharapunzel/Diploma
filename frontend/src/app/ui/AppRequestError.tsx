import { ApiError, errorText } from "../api/client";
import { RequestError } from "../../shared/ui/RequestError";

type Props = {
  error: unknown;
  onRetry?: () => void;
  message?: string;
  actionLabel?: string;
  variant?: "inline" | "page";
};

export function AppRequestError({ error, message, ...props }: Props) {
  const safeMessage = message ?? errorText(error);
  const requestId = error instanceof ApiError ? error.requestId : null;
  return (
    <RequestError message={safeMessage} requestId={requestId} {...props} />
  );
}
