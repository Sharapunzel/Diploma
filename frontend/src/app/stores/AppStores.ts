import { ApiClient } from "../api/client";
import { LayoutStore } from "./LayoutStore";
import { SessionStore } from "./SessionStore";
import { ThemeStore } from "./ThemeStore";
import { DetectorWorkspace } from "../../features/detectors/Workspace";

export class AppStores {
  api = new ApiClient();
  theme = new ThemeStore();
  session = new SessionStore(this.api);
  layout = new LayoutStore();
  detectors = new DetectorWorkspace(this.api, this.session);

  dispose() {
    this.detectors.dispose();
    this.theme.dispose();
    this.session.dispose();
  }
}
