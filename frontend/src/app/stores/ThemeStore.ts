import { makeAutoObservable } from "mobx";
import type { ThemeChoice } from "../../shared/types/theme";

export type { ThemeChoice } from "../../shared/types/theme";

export class ThemeStore {
  choice: ThemeChoice = "system";
  dark = false;
  private media: MediaQueryList | null = null;
  private listener = () => {
    if (this.choice === "system") this.update();
  };

  constructor() {
    makeAutoObservable(this, { media: false, listener: false } as never, {
      autoBind: true,
    });
    try {
      const saved = localStorage.getItem("theme");
      if (saved === "light" || saved === "dark" || saved === "system")
        this.choice = saved;
    } catch {
      // The preference is optional when storage is blocked.
    }
    if (typeof matchMedia === "function") {
      this.media = matchMedia("(prefers-color-scheme: dark)");
      this.media.addEventListener("change", this.listener);
    }
    this.update();
  }

  private update() {
    const dark =
      this.choice === "dark" ||
      (this.choice === "system" && !!this.media?.matches);
    if (typeof document !== "undefined")
      document.documentElement.dataset.theme = dark ? "dark" : "light";
    this.dark = dark;
  }

  choose(value: ThemeChoice) {
    this.choice = value;
    this.update();
    try {
      localStorage.setItem("theme", value);
    } catch {
      // The preference still applies for the current page.
    }
  }

  dispose() {
    this.media?.removeEventListener("change", this.listener);
  }
}
