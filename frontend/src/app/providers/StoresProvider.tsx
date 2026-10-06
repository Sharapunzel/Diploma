import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { AppStores } from "../stores/AppStores";

const StoresContext = createContext<AppStores | null>(null);

export function StoresProvider({
  children,
  stores: provided,
}: {
  children: ReactNode;
  stores?: AppStores;
}) {
  const [stores] = useState(() => provided ?? new AppStores());
  useEffect(() => () => stores.dispose(), [stores]);
  return (
    <StoresContext.Provider value={stores}>{children}</StoresContext.Provider>
  );
}

export function useStores(): AppStores {
  const stores = useContext(StoresContext);
  if (!stores) throw new Error("StoresProvider is missing");
  return stores;
}
