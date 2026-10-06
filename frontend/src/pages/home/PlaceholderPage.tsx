import { useState } from "react";
import { Button } from "antd";
import { Link } from "react-router-dom";
import { LoadingOverlay } from "../../shared/ui/LoadingOverlay";
import styles from "./PlaceholderPage.module.css";

export function PlaceholderPage({
  name,
  notFound = false,
  demoLoading = false,
}: {
  name: string;
  notFound?: boolean;
  demoLoading?: boolean;
}) {
  const [loading, setLoading] = useState(false);
  return (
    <div className={styles.stage}>
      <section
        aria-labelledby="placeholder-title"
        className={styles.panel}
        data-testid="placeholder-panel"
      >
        <p className={styles.eyebrow}>{notFound ? "Ошибка 404" : name}</p>
        <h2 className={styles.title} id="placeholder-title">
          {notFound ? "Страница не найдена" : "Раздел готовится"}
        </h2>
        <p className={styles.description}>
          {notFound
            ? "Проверьте адрес или вернитесь на главную страницу."
            : "Этот раздел появится в следующей задаче проекта."}
        </p>
        {demoLoading && (
          <Button
            aria-pressed={loading}
            className={styles.demoButton}
            onClick={() => setLoading((current) => !current)}
          >
            {loading ? "Скрыть загрузку" : "Показать загрузку"}
          </Button>
        )}
        {notFound && (
          <Link className={styles.homeLink} to="/">
            На главную
          </Link>
        )}
      </section>
      {loading && <LoadingOverlay />}
    </div>
  );
}
