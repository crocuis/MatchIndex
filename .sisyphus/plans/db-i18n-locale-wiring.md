# DB I18n Locale Wiring

## Scope

Implement only DB-backed locale wiring for nation, league, and club names already supported by translation tables.

1. Remove remaining `locale = 'en'` hardcoding from relevant DB translation lookups.
2. Pass request locale from pages to DB functions where missing.
3. Keep current English fallback behavior when locale-specific rows are absent.
4. Do not add schema, new features, or new UI.

## Acceptance Criteria

1. Nation, league, and club names returned by DB queries use requested locale first, then English, then existing slug/code fallback.
2. Pages that render those names pass `getLocale()` into the relevant DB functions.
3. `npx tsc --noEmit` succeeds.
4. `npm run build` succeeds.
5. Manual QA: fetching key pages with `MATCHINDEX_LOCALE=ko` cookie shows DB-backed Korean names where translation rows exist, otherwise English fallback.

## Minimal Implementation Steps

1. Update locale-aware SQL joins and translation subqueries in `src/data/postgres.ts` for match, standings, and supporting entity queries.
2. Update app route calls that currently omit locale when fetching leagues, clubs, nations, matches, and standings.
3. Run typecheck and build.
4. Manually fetch key pages under Korean locale and verify rendered names.

## TDD-Oriented Verification

1. Baseline check current HTML output for key pages using a `MATCHINDEX_LOCALE=ko` cookie.
2. Apply locale wiring changes.
3. Re-fetch the same pages with the same cookie and confirm DB-backed names differ where Korean rows exist.

## Manual QA Scenario

1. Use `curl` or equivalent fetch with `Cookie: MATCHINDEX_LOCALE=ko`.
2. Verify non-prefixed routes such as `/nations`, `/leagues`, `/clubs`, and one detail page for each entity type.
3. Expected result: DB-backed Korean nation/league/club names when a `ko` translation row exists; English fallback otherwise.

## Atomic Commit Strategy

1. `postgres locale wiring`
2. `page locale propagation`
3. `verification fixes only`
