import { AppstoreOutlined, HomeOutlined } from "@ant-design/icons";
import { Menu, type MenuProps } from "antd";
import { observer } from "mobx-react-lite";
import { Link, useLocation } from "react-router-dom";
import { availableNavigationGroups } from "../../app/routes/permissions";
import { useStores } from "../../app/providers/StoresProvider";
import styles from "./Navigation.module.css";

export const Navigation = observer(
  ({
    mobile = false,
    showLabels = true,
  }: {
    mobile?: boolean;
    showLabels?: boolean;
  }) => {
    const { session, layout } = useStores();
    const location = useLocation();
    const groups = availableNavigationGroups((permission) =>
      session.has(permission),
    );

    if (!mobile && !showLabels) {
      return (
        <nav
          aria-label="Основное меню"
          className={`${styles.navigation} ${styles.compactNavigation}`}
          data-testid="compact-navigation"
        >
          {groups.map((group, index) => (
            <div className={styles.compactGroup} key={group.label}>
              {index > 0 && (
                <div
                  aria-label={group.label}
                  className={styles.compactDivider}
                  role="separator"
                />
              )}
              {group.pages.map((page) => (
                <Link
                  aria-current={
                    location.pathname === page.path ? "page" : undefined
                  }
                  aria-label={page.label}
                  className={styles.compactLink}
                  key={page.path}
                  title={page.label}
                  to={page.path}
                >
                  {page.path === "/" ? <HomeOutlined /> : <AppstoreOutlined />}
                </Link>
              ))}
            </div>
          ))}
        </nav>
      );
    }

    const links: NonNullable<MenuProps["items"]> = [];
    groups.forEach((group) => {
      const children = group.pages.map((page) => ({
        key: page.path,
        label: (
          <Link
            aria-label={page.label}
            to={page.path}
            onClick={() => mobile && layout.setMobileOpen(false)}
          >
            {showLabels && <span className={styles.label}>{page.label}</span>}
          </Link>
        ),
        title: page.label,
        icon: page.path === "/" ? <HomeOutlined /> : <AppstoreOutlined />,
      }));
      if (group.pages[0]?.path === "/") {
        links.push(...children);
        return;
      }
      links.push({
        type: "group",
        key: group.label,
        label: group.label,
        children,
      });
    });

    return (
      <nav
        aria-label={mobile ? "Мобильное меню" : "Основное меню"}
        className={styles.navigation}
      >
        <Menu
          className={styles.menu}
          selectedKeys={[location.pathname]}
          items={links}
          mode="inline"
        />
      </nav>
    );
  },
);
