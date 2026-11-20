"""Persistence of a single active window context; caller holds schedule mutex."""

from sqlalchemy import delete

from ...models import AggregateHistorySpan, AggregateWindowAttempt


class SqlAlchemyAggregateAttemptQueries:
    def history_context(self, aggregate_id, cohort_key, budget):
        self.read_budget(budget.milliseconds())
        span = self.session.get(AggregateHistorySpan, (aggregate_id, cohort_key), populate_existing=True)
        return {"load_start": span.load_start, "load_end": span.load_end, "coordinates": span.coordinates} if span else None

    def window_attempt(self, schedule_id, budget):
        self.read_budget(budget.milliseconds())
        return self.session.get(AggregateWindowAttempt, schedule_id, populate_existing=True)

    def add_window_attempt(self, attempt, budget):
        self.read_budget(budget.milliseconds())
        self.session.add(attempt)
        self.session.flush()

    def revoke_window_attempt(self, schedule_id, budget=None):
        if budget is not None:
            self.read_budget(budget.milliseconds())
        self.session.execute(delete(AggregateWindowAttempt).where(AggregateWindowAttempt.schedule_id == schedule_id))
