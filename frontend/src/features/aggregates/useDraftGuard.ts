import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { runInAction } from "mobx";
import type { DraftStore } from "./DraftStore";

export function useDraftGuard(store: DraftStore) {
  const navigate = useNavigate();
  const location = useLocation();
  const [leaving, setLeaving] = useState<(() => void) | null>(null);
  useEffect(() => {
    const currentIndex = window.history.state?.idx as number | undefined;
    let restoring = false;
    const leave = (task: () => void) => () => {
      runInAction(() => {
        store.dirty = false;
      });
      task();
    };
    const unload = (e: BeforeUnloadEvent) => {
      if (store.dirty) {
        e.preventDefault();
        e.returnValue = "";
      }
    };
    const click = (e: MouseEvent) => {
      const a = (e.target as Element).closest?.(
        "a[href]",
      ) as HTMLAnchorElement | null;
      if (
        !store.dirty ||
        !a ||
        a.origin !== window.location.origin ||
        a.getAttribute("href")?.startsWith("#") ||
        e.ctrlKey ||
        e.metaKey ||
        a.target === "_blank"
      )
        return;
      e.preventDefault();
      e.stopPropagation();
      setLeaving(() => leave(() => navigate(a.pathname + a.search)));
    };
    const pop = (e: PopStateEvent) => {
      if (restoring) {
        restoring = false;
        return;
      }
      if (
        !store.dirty ||
        typeof currentIndex !== "number" ||
        typeof e.state?.idx !== "number"
      )
        return;
      const delta = currentIndex - e.state.idx;
      if (!delta) return;
      // Restore before React Router observes the navigation; confirm only the user's requested step.
      e.stopImmediatePropagation();
      restoring = true;
      window.history.go(delta);
      setLeaving(() => leave(() => window.history.go(-delta)));
    };
    window.addEventListener("beforeunload", unload);
    document.addEventListener("click", click, true);
    window.addEventListener("popstate", pop, true);
    return () => {
      window.removeEventListener("beforeunload", unload);
      document.removeEventListener("click", click, true);
      window.removeEventListener("popstate", pop, true);
    };
  }, [store, navigate, location.key]);
  return {
    leaving,
    cancelLeave: () => setLeaving(null),
    confirmLeave: () => {
      leaving?.();
      setLeaving(null);
    },
  };
}
