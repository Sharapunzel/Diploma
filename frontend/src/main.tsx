import "@fontsource/play/400.css";
import "@fontsource/play/700.css";
import { createRoot } from "react-dom/client";
import { App } from "./app/App";
import { StoresProvider } from "./app/providers/StoresProvider";
import "./app/styles.css";

createRoot(document.getElementById("root")!).render(
  <StoresProvider>
    <App />
  </StoresProvider>,
);
