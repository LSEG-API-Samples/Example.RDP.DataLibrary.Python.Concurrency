---
name: ld-historical-pricing-async
description: "Convert LSEG Data Library synchronous single-RIC loops to concurrent Historical Pricing requests. Use when: migrating sequential events or summaries loops; fetching historical pricing for multiple RICs concurrently; applying asyncio.gather() or asyncio.TaskGroup(); controlling concurrency and rate limits with asyncio.Semaphore; handling HTTP 429 Too Many Requests errors; implementing response display helpers for async results."
argument-hint: "number of instruments or RIC list"
---

# LSEG Data Library — Async Historical Pricing Pattern

Converts sequential single-RIC `historical_pricing.events.Definition(...).get_data()` and `historical_pricing.summaries.Definition(...).get_data()` loops into concurrent async requests using `asyncio.gather()` or `asyncio.TaskGroup()`.

---

## When to Use

- Replacing a `for ric in INSTRUMENTS: historical_pricing.<definition>.Definition(universe=ric, ...).get_data()` loop
- Fetching historical pricing for many RICs at once (latency dominated by network I/O)
- Demonstrating async performance improvement vs synchronous baseline
- Handling per-request errors without aborting the whole batch
- HTTP 429 rate-limit errors indicate too much concurrency — add a semaphore

---

## Key API Difference

| Synchronous (baseline) | Async (optimized) |
|---|---|
| `historical_pricing.summaries.Definition(universe=ric, ...).get_data()` | `historical_pricing.summaries.Definition(universe=ric, ...).get_data_async(closure=label)` |
| Returns a response object synchronously, one RIC at a time | Returns a response object; all RICs run concurrently |
| `.data.df`, `.is_success`, `.http_status` available immediately | Same fields available after `await` |
| Called in a `for` loop | Dispatched via `asyncio.gather()` or `asyncio.TaskGroup()` |

---

## Procedure

### Step 1 — Update Imports

```python
import asyncio
import time
from IPython.display import Markdown, display
from lseg.data.content import historical_pricing
from lseg.data.content.historical_pricing import Adjustments, Intervals
from lseg.data._errors import LDError
import pandas as pd
pd.set_option("future.no_silent_downcasting", True)
```

Remove `import time` and the `Markdown`/`display` imports only if the timing and notebook display lines are removed from the examples.

### Step 2 — Use Typed Adjustment Enums

The `historical_pricing` Content Layer uses `Adjustments` enums, not raw strings. Use a neutral name when the same adjustments apply to summaries as well as events:

```python
ADJUSTMENTS = [
    Adjustments.EXCHANGE_CORRECTION,
    Adjustments.MANUAL_CORRECTION,
    Adjustments.CCH,
    Adjustments.CRE,
    Adjustments.RPO,
    Adjustments.RTS,
]
```

### Step 3a — Add a Gather Response Helper

This function handles both successful and failed responses returned by `return_exceptions=True`:

```python
def display_response(data):
    """Display the result of each async API call.

    For each response:
    - Prints the exception message if the task raised a Python error.
    - Warns if the response has no closure label (unexpected type).
    - Renders the DataFrame on a successful HTTP response.
    - Prints the HTTP status code on a failed (4xx/5xx) response.
    """
    for api_response in data:
        if isinstance(api_response, Exception):
            print(f"\nTask failed with exception: {api_response}")
            continue

        if not hasattr(api_response, "closure"):
            print(f"\nUnexpected response type: {type(api_response)}")
            continue

        print(f"\nResponse received for: {api_response.closure}")

        if api_response.is_success:
            display(api_response.data.df)
        else:
            print(f"Request failed — HTTP status: {api_response.http_status}")
```

### Step 3b — Add a TaskGroup Response Helper

After a `TaskGroup` exits successfully, every retained task result is a response object, not an exception returned by `gather(..., return_exceptions=True)`. Use a simpler helper for that contract:

```python
def display_response_taskgroup(data):
    """Display completed asyncio.TaskGroup response objects."""
    for index, api_response in enumerate(data, start=1):
        closure = getattr(api_response, "closure", f"result-{index}")
        print(f"\n[{index}] Response received for: {closure}")

        if api_response.is_success:
            display(api_response.data.df)
        else:
            print(f"Request failed — HTTP status: {api_response.http_status}")
```

### Step 4a — Pattern A: `asyncio.gather()` (Resilient, Continue-on-Error)

Best for: processing all instruments even if some fail (e.g., invalid RICs, 404s).

```python
hist_data = []
display(Markdown("**Start the wall-clock timer...**"))
start_time = time.perf_counter()
try:
    tasks = asyncio.gather(
        *[
            historical_pricing.summaries.Definition(
                universe=ric,
                fields=INTERDAY_FIELDS,
                interval=Intervals.DAILY,
                start=START,
                end=END,
                adjustments=ADJUSTMENTS,
            ).get_data_async(closure=ric)
            for ric in INSTRUMENTS
        ],
        return_exceptions=True,  # Collect all results; don't abort on first error
    )
    hist_data = await tasks  # pylint: disable=await-outside-async
    display_response(hist_data)
except LDError as error:
    # Catches failures raised while constructing or starting the batch
    # (e.g. invalid Definition arguments) — not per-request errors, which
    # are already captured in hist_data via return_exceptions=True.
    print(f"Error: {error}")
finally:
    elapsed = time.perf_counter() - start_time
    display(Markdown(
        f"**Fetched ({len(INSTRUMENTS)}) RICs concurrently in {elapsed:0.2f} seconds.**"
    ))
```

With `return_exceptions=True`, exceptions raised by individual request coroutines are returned in `hist_data`; `display_response()` handles them item by item. The `except LDError` clause only catches failures that occur while constructing or starting the batch (before `gather()` schedules the coroutines).

### Step 4b — Pattern B: `asyncio.TaskGroup()` (Fail-Fast, Python 3.11+)

Best for: all-or-nothing batches where a single failure should abort the group.

```python
hist_data = []
display(Markdown("**Start the wall-clock timer...**"))
start_time = time.perf_counter()
try:
    async with asyncio.TaskGroup() as tg:
        task_handles = [
            tg.create_task(
                historical_pricing.summaries.Definition(
                    universe=ric,
                    fields=INTERDAY_FIELDS,
                    interval=Intervals.DAILY,
                    start=START,
                    end=END,
                    adjustments=ADJUSTMENTS,
                ).get_data_async(closure=ric)
            )
            for ric in INSTRUMENTS
        ]
    hist_data = [t.result() for t in task_handles]
    display_response_taskgroup(hist_data)
except* LDError as errors:
    for error in errors.exceptions:
        print(f"Error: {error}")
finally:
    elapsed = time.perf_counter() - start_time
    display(Markdown(
        f"**Fetched ({len(INSTRUMENTS)}) RICs concurrently in {elapsed:0.2f} seconds.**"
    ))
```

### Step 5 — Control Concurrency with `asyncio.Semaphore` (HTTP 429 Prevention)

#### Why

`asyncio.gather()` and `asyncio.TaskGroup()` schedule every coroutine at once. Large batches may exceed your account's platform rate limit and produce HTTP **429 Too Many Requests** responses. An `asyncio.Semaphore` acts as a counter that allows at most *N* coroutines to hold it simultaneously; all others pause and wait their turn.

```
asyncio.Semaphore(N)  →  at most N HTTP requests in-flight at any time
```

#### Key Rules

- **Create the semaphore once**, outside the helper function, and close over it. Never create a new instance per call.
- **Wrap only the `await get_data_async(...)` call** inside `async with semaphore:`. That is the only critical section that needs throttling.
- **The semaphore controls concurrency inside each coroutine**, not inside `asyncio.gather()` itself. Pass the coroutine list to `gather()` as usual.
- **Start conservatively** (5–10) and increase only if the platform responds cleanly. Reduce further if 429s persist.

#### Pattern with `asyncio.gather()`

```python
MAX_CONCURRENT = 5  # Tune down if 429s persist; tune up if platform allows
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

async def fetch_with_limit(ric: str):
    """Fetch one RIC's historical data, respecting the concurrency cap."""
    async with semaphore:  # Suspends here when MAX_CONCURRENT requests are already in-flight
        return await historical_pricing.summaries.Definition(
            universe=ric,
            fields=INTERDAY_FIELDS,
            interval=Intervals.DAILY,
            start=START,
            end=END,
            adjustments=ADJUSTMENTS,
        ).get_data_async(closure=ric)

try:
    tasks = asyncio.gather(
        *[fetch_with_limit(ric) for ric in INSTRUMENTS],
        return_exceptions=True,  # Collect all results; don't abort on first error
    )
    hist_data = await tasks  # pylint: disable=await-outside-async
    display_response(hist_data)
except LDError as error:
    # Catches failures raised while constructing or starting the batch;
    # per-request errors are already captured in hist_data above.
    print(f"Error: {error}")
```

#### Pattern with `asyncio.TaskGroup()` (Python 3.11+)

```python
MAX_CONCURRENT = 5
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

async def fetch_with_limit(ric: str):
    """Fetch one RIC's historical data, respecting the concurrency cap."""
    async with semaphore:
        return await historical_pricing.summaries.Definition(
            universe=ric,
            fields=INTERDAY_FIELDS,
            interval=Intervals.DAILY,
            start=START,
            end=END,
            adjustments=ADJUSTMENTS,
        ).get_data_async(closure=ric)

try:
    async with asyncio.TaskGroup() as tg:
        task_handles = [
            tg.create_task(fetch_with_limit(ric))
            for ric in INSTRUMENTS
        ]
    hist_data = [t.result() for t in task_handles]
    display_response_taskgroup(hist_data)
except* LDError as errors:
    for error in errors.exceptions:
        print(f"Error: {error}")
```

#### Tuning Guide

| Situation | Action |
|---|---|
| HTTP 429 errors appear | Reduce `MAX_CONCURRENT` (try 3–5) |
| Requests complete cleanly and quickly | Gradually increase `MAX_CONCURRENT` (try 10–15) |
| Safe starting point for most accounts | `MAX_CONCURRENT = 5` to `10` |
| Only a few RICs (< 10) | Semaphore is optional; the platform can usually handle it |

---

## Decision: `gather()` vs `TaskGroup()`

| | `asyncio.gather()` | `asyncio.TaskGroup()` |
|---|---|---|
| On coroutine exception | Continues collecting all results (`return_exceptions=True`) | Cancels unfinished sibling tasks and raises an `ExceptionGroup` |
| Result format | `list` returned by `await tasks` | `[t.result() for t in task_handles]` after block exits |
| Error handling | Check `isinstance(r, Exception)` per item | Catch `except* LDError` ExceptionGroup |
| Python version | 3.7+ | 3.11+ |
| Best for | Mixed valid/invalid RIC lists and batch reporting | All-or-nothing work where coroutine exceptions should stop the batch |

---

## Common Issues

| Symptom | Cause | Fix |
|---|---|---|
| HTTP 429 | Too many concurrent requests | Add semaphore (Step 5), reduce `MAX_CONCURRENT` |
| `await outside async` lint warning | Jupyter runs in an event loop | Add `# pylint: disable=await-outside-async` comment |
| `AttributeError: 'str' object has no attribute 'EXCHANGE_CORRECTION'` | Raw strings passed to `adjustments=` | Use `Adjustments.*` enums (Step 2) |
| Empty `.data.df` | Invalid RIC or date range outside market data | Check `api_response.is_success` and `http_status` |
| `ExceptionGroup` not caught | `except LDError` used instead of `except*` | Change to `except* LDError` |
