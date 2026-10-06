import { makeAutoObservable } from "mobx";

export class LayoutStore {
  collapsed = false;
  mobileOpen = false;
  accountOpen = false;

  constructor() {
    makeAutoObservable(this, {}, { autoBind: true });
  }

  toggleCollapsed() {
    this.collapsed = !this.collapsed;
  }

  setMobileOpen(value: boolean) {
    this.mobileOpen = value;
  }

  setAccountOpen(value: boolean) {
    this.accountOpen = value;
  }
}
