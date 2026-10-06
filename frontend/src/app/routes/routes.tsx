import { lazy, Suspense, type ReactNode } from "react";
import { Alert, Spin } from "antd";
import { observer } from "mobx-react-lite";
import {
  BrowserRouter,
  Navigate,
  Route,
  Routes,
  useLocation,
} from "react-router-dom";
import { useStores } from "../providers/StoresProvider";
import { AppLayout } from "../../widgets/layout/AppLayout";
import { AppRequestError } from "../ui/AppRequestError";
import { ThemeToggleControl } from "../ui/ThemeToggleControl";
import styles from "./routes.module.css";
import { homePage, pages } from "./permissions";

const LoginPage = lazy(() =>
  import("../../pages/login/LoginPage").then(({ LoginPage }) => ({
    default: LoginPage,
  })),
);
const PlaceholderPage = lazy(() =>
  import("../../pages/home/PlaceholderPage").then(({ PlaceholderPage }) => ({
    default: PlaceholderPage,
  })),
);

function LoadingStatus() {
  return (
    <div className={styles.statusPage} data-testid="session-status-page">
      <ThemeToggleControl className={styles.theme} />
      <section
        aria-busy="true"
        className={styles.statusCard}
        data-testid="session-status-card"
      >
        <Spin size="large" aria-label="Проверка сессии" />
      </section>
      <footer className={styles.statusVersion} data-testid="status-version">
        v{__APP_VERSION__}
      </footer>
    </div>
  );
}

function ErrorStatus({
  error,
  onRetry,
}: {
  error: unknown;
  onRetry: () => void;
}) {
  return (
    <div className={styles.statusPage} data-testid="session-status-page">
      <ThemeToggleControl className={styles.theme} />
      <section
        className={`${styles.statusCard} ${styles.errorCard}`}
        data-testid="session-status-card"
      >
        <AppRequestError error={error} onRetry={onRetry} variant="page" />
      </section>
      <footer className={styles.statusVersion} data-testid="status-version">
        v{__APP_VERSION__}
      </footer>
    </div>
  );
}

export const Protected = observer(
  ({ permission, children }: { permission?: string; children: ReactNode }) => {
    const { session } = useStores();
    const location = useLocation();
    if (session.state === "loading") return <LoadingStatus />;
    if (session.state === "error") {
      return (
        <ErrorStatus
          error={session.error}
          onRetry={() => void session.refresh(true)}
        />
      );
    }
    if (session.state === "anonymous") {
      return (
        <Navigate to="/login" state={{ from: location.pathname }} replace />
      );
    }
    return (
      <AppLayout>
        {permission && !session.has(permission) ? (
          <Alert
            type="warning"
            message="Недостаточно прав для просмотра раздела"
          />
        ) : (
          children
        )}
      </AppLayout>
    );
  },
);

const LoginRoute = observer(() => {
  const { session } = useStores();
  if (session.state === "loading") return <LoadingStatus />;
  if (session.state === "error") {
    return (
      <ErrorStatus
        error={session.error}
        onRetry={() => void session.refresh(true)}
      />
    );
  }
  return <LoginPage />;
});

export function AppRoutes() {
  return (
    <BrowserRouter>
      <Suspense fallback={<LoadingStatus />}>
        <Routes>
          <Route path="/login" element={<LoginRoute />} />
          <Route
            path={homePage.path}
            element={
              <Protected>
                <PlaceholderPage name={homePage.label} demoLoading />
              </Protected>
            }
          />
          {pages.map((page) => (
            <Route
              key={page.path}
              path={page.path}
              element={
                <Protected permission={page.permission}>
                  <PlaceholderPage name={page.label} />
                </Protected>
              }
            />
          ))}
          <Route
            path="*"
            element={
              <Protected>
                <PlaceholderPage name="Страница не найдена" notFound />
              </Protected>
            }
          />
        </Routes>
      </Suspense>
    </BrowserRouter>
  );
}
