import { makeAutoObservable, observable, runInAction } from "mobx";
import type {
  AggregatesApi,
  FieldCatalog,
  SourceField,
} from "../../app/api/aggregates";
import { RequestLifecycle } from "../resources/RequestLifecycle";

export class CatalogConsistencyError extends Error {}

export class CatalogStore {
  page: FieldCatalog | null = null;
  text = "";
  error: unknown = null;
  lifecycle = new RequestLifecycle();
  constructor(
    private api: AggregatesApi,
    private source: string,
  ) {
    makeAutoObservable(this, { api: false, page: observable.ref } as never, {
      autoBind: true,
    });
  }
  async search(q = "") {
    this.lifecycle.cancel("search");
    return this.lifecycle.once("search", async () => {
      const r = this.lifecycle.begin("query");
      this.error = null;
      try {
        const page = await this.api.fieldCatalog(this.source, q, 0, r.signal);
        if (r.current())
          runInAction(() => {
            this.page = page;
          });
      } catch (error) {
        if (r.current())
          runInAction(() => {
            this.error = error;
            this.page = null;
          });
      }
    });
  }
  async fullText(): Promise<string | undefined> {
    if (!this.source) return;
    return this.lifecycle.once("full", async () => {
      const r = this.lifecycle.begin("full-catalog");
      this.error = null;
      this.text = "";
      try {
        const fields: SourceField[] = [],
          seen = new Set<string>();
        let offset = 0,
          hash = "",
          total: number | null = null;
        do {
          const page = await this.api.fieldCatalog(
            this.source,
            "",
            offset,
            r.signal,
            100,
          );
          if (!r.current()) return;
          if (
            (hash && hash !== page.catalog_hash) ||
            (total !== null && total !== page.total) ||
            page.offset !== offset ||
            page.items.length > page.limit ||
            (offset < page.total && !page.items.length)
          )
            throw new CatalogConsistencyError(
              "Каталог изменился или получен не полностью. Повторите копирование явно.",
            );
          hash = page.catalog_hash;
          total = page.total;
          for (const field of page.items) {
            if (seen.has(field.name))
              throw new CatalogConsistencyError(
                "Повтор поля в каталоге. Повторите загрузку.",
              );
            seen.add(field.name);
            fields.push(field);
          }
          offset += page.items.length;
          if (offset > total || (offset === 0 && total !== 0))
            throw new CatalogConsistencyError(
              "Каталог получен не полностью. Повторите копирование явно.",
            );
        } while (total !== null && offset < total);
        const text = fields.length
          ? fields
              .map(
                (f) =>
                  `${f.name} · ${f.type}${f.is_array ? "[]" : ""} · ${f.description} · ${f.provenance.map((p) => (p === "observed" ? "Обнаружено в событиях" : "Предусмотрено нормализатором")).join(" + ")}${f.normalizer_origin.length ? ` (${f.normalizer_origin.join(", ")})` : ""} · ${f.supported ? "поддерживается" : `не поддерживается: ${f.reason ?? "тип"}`}`,
              )
              .join("\n")
          : "Каталог пуст: нет наблюдавшихся или предусмотренных нормализатором ECS-полей.";
        if (r.current()) {
          runInAction(() => {
            this.text = text;
          });
          return text;
        }
      } catch (error) {
        if (r.current())
          runInAction(() => {
            this.error = error;
          });
      }
    });
  }
  dispose() {
    this.lifecycle.dispose();
    this.page = null;
    this.text = "";
    this.error = null;
  }
}
