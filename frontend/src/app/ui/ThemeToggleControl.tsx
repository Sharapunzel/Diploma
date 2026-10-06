import { observer } from "mobx-react-lite";
import { useStores } from "../providers/StoresProvider";
import { ThemeToggle } from "../../shared/ui/ThemeToggle";

export const ThemeToggleControl = observer(
  ({ className = "" }: { className?: string }) => {
    const { theme } = useStores();
    return (
      <ThemeToggle
        choice={theme.choice}
        className={className}
        onChange={theme.choose}
      />
    );
  },
);
