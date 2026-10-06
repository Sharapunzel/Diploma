import styles from "./Logo.module.css";

export function Logo() {
  return (
    <img
      alt="Логотип"
      className={styles.logo}
      height="38"
      src="/pictures/logoGreen.svg"
      width="38"
    />
  );
}
