import { ApiError, errorText } from "../api/client";
import { RequestError } from "../../shared/ui/RequestError";

type Props = {
  error: unknown;
  onRetry?: () => void;
  variant?: "inline" | "page";
};

export function AppRequestError({ error, ...props }: Props) {
  const message = errorText(error);
  const requestId = error instanceof ApiError ? error.requestId : null;
  return <RequestError message={message} requestId={requestId} {...props} />;
}
