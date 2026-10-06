import { ApiClient } from "../api/client";
import { LayoutStore } from "./LayoutStore";
import { SessionStore } from "./SessionStore";
import { ThemeStore } from "./ThemeStore";

export class AppStores {
  api = new ApiClient();
  theme = new ThemeStore();
  session = new SessionStore(this.api);
  layout = new LayoutStore();

  dispose() {
    this.theme.dispose();
    this.session.dispose();
  }
}
