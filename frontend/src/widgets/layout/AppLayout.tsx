import { useEffect, useRef, useState, type ReactNode } from "react";
import {
  MenuFoldOutlined,
  MenuOutlined,
  MenuUnfoldOutlined,
} from "@ant-design/icons";
import { Button, Drawer, Layout } from "antd";
import { observer } from "mobx-react-lite";
import { useLocation } from "react-router-dom";
import { homePage, pages } from "../../app/routes/permissions";
import { useStores } from "../../app/providers/StoresProvider";
import { Logo } from "../../shared/ui/Logo";
import { AccountPanel } from "../account/AccountPanel";
import { Navigation } from "./Navigation";
import styles from "./AppLayout.module.css";

export const AppLayout = observer(({ children }: { children: ReactNode }) => {
  const { layout } = useStores();
  const location = useLocation();
  const main = useRef<HTMLElement>(null);
  const mobileTrigger = useRef<HTMLButtonElement>(null);
  const [labelsVisible, setLabelsVisible] = useState(!layout.collapsed);
  const currentPage =
    location.pathname === homePage.path
      ? homePage.label
      : (pages.find((page) => page.path === location.pathname)?.label ??
        "Страница не найдена");

  useEffect(() => {
    layout.setMobileOpen(false);
    requestAnimationFrame(() => main.current?.focus());
  }, [location.pathname, layout]);

  useEffect(() => {
    if (layout.collapsed) {
      setLabelsVisible(false);
      return;
    }
    const delay = window.matchMedia("(prefers-reduced-motion: reduce)").matches
      ? 0
      : 230;
    const timeout = window.setTimeout(() => setLabelsVisible(true), delay);
    return () => window.clearTimeout(timeout);
  }, [layout.collapsed]);

  const closeMobile = () => {
    layout.setMobileOpen(false);
    requestAnimationFrame(() => mobileTrigger.current?.focus());
  };

  return (
    <Layout
      className={styles.shell}
      data-collapsed={layout.collapsed}
      data-testid="workspace-shell"
    >
      <a
        className={styles.skipLink}
        href="#main"
        onClick={(event) => {
          event.preventDefault();
          main.current?.focus();
        }}
      >
        К основному содержимому
      </a>
      <Layout.Sider
        className={styles.sidebar}
        collapsed={layout.collapsed}
        collapsedWidth={76}
        trigger={null}
        width={248}
      >
        <div
          aria-label="Diploma"
          className={styles.brand}
          data-testid="workspace-brand"
        >
          <Logo />
          {labelsVisible && !layout.collapsed && <span>Diploma</span>}
        </div>
        <button
          aria-expanded={!layout.collapsed}
          aria-label={layout.collapsed ? "Развернуть меню" : "Свернуть меню"}
          className={styles.collapseButton}
          onClick={() => {
            setLabelsVisible(false);
            layout.toggleCollapsed();
          }}
          type="button"
        >
          <span aria-hidden="true" className={styles.collapseGlyph}>
            {layout.collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
          </span>
          {labelsVisible && !layout.collapsed && <span>Свернуть меню</span>}
        </button>
        <Navigation showLabels={labelsVisible && !layout.collapsed} />
        <footer className={styles.sidebarFooter} data-testid="app-version">
          v{__APP_VERSION__}
        </footer>
      </Layout.Sider>
      <Layout className={styles.mainLayout}>
        <Layout.Header className={styles.header} data-testid="workspace-header">
          <h1
            className={styles.pageTitle}
            data-testid="workspace-title"
            title={currentPage}
          >
            {currentPage}
          </h1>
          <Button
            ref={mobileTrigger}
            aria-label="Открыть меню"
            aria-expanded={layout.mobileOpen}
            className={styles.mobileToggle}
            icon={<MenuOutlined />}
            onClick={() => layout.setMobileOpen(true)}
          />
          <AccountPanel />
        </Layout.Header>
        <Layout.Content
          className={styles.content}
          data-testid="workspace-content"
          id="main"
          ref={main}
          tabIndex={-1}
        >
          {children}
        </Layout.Content>
      </Layout>
      <Drawer
        classNames={{
          body: styles.drawerBody,
          content: styles.drawerContent,
          header: styles.drawerHeader,
        }}
        onClose={closeMobile}
        open={layout.mobileOpen}
        placement="left"
        rootClassName={styles.drawerRoot}
        title="Разделы"
        width={300}
      >
        <div aria-label="Diploma" className={styles.drawerBrand}>
          <Logo />
          <span>Diploma</span>
        </div>
        <Navigation mobile />
      </Drawer>
    </Layout>
  );
});
