# Importing and running the hackathon data prep

One file to import, one notebook to open. Everything else runs itself.

## What you are importing

`ageas_hackathon_prep.dbc` is a Databricks archive holding three notebooks. It imports as a
folder, which matters: `00_prep_session` calls the other two by relative path, so they have
to stay together in the same folder.

```
ageas_hackathon_prep/
  00_prep_session              <- the one you open
  01_generate_hackathon_data   <- called for you
  02_generate_documents        <- called for you
  07_tutorial_smoke_test       <- open this second, once the data is in
```

## Before you start

Three things, because without them the session cannot begin:

1. Access to the Ageas sandbox workspace.
2. Permission to create a schema and a volume in the target catalog, which we expect to be
   `sb_training`. If that is not possible, any catalog you can write to is fine for now and
   we can move it later.
3. Compute you can attach a notebook to. A cluster is fine. Databricks Runtime 16.4 or
   above is needed for the metric views, and 17.3 or above for the document parsing.

## Import it

1. In the workspace sidebar, click **Workspace**.
2. Navigate to the folder you want it in, for example your own user folder.
3. Click the kebab menu, then **Import**.
4. Choose **File**, select `ageas_hackathon_prep.dbc`, and click **Import**.

You should now see a folder with the three notebooks in it.

## Run it

1. Open **`00_prep_session`**. Do not open the other two.
2. Attach it to your compute.
3. Set the five widgets at the top:

| Widget | Value |
| --- | --- |
| 1. Catalog | `sb_training`, or whichever catalog you can write to |
| 2. Schema | `hackathon` |
| 3. Volume | `landing` |
| 4. Scale | `full` |
| 5. Run the generators | `yes` |

4. **Run All.** It takes around fifteen minutes at full scale.

If you would rather confirm the environment first, set **Scale** to `smoke`. That builds a
small version in about two minutes and every check still runs. Then set it back to `full`
and run it again.

## What it does, in order

1. Checks the environment and reports anything that would stop the day.
2. Runs the two generator notebooks.
3. Verifies what landed: row counts, column comments, referential integrity, and whether
   the numbers look like a real insurance book.
4. Prints a sign-off table with a single verdict at the end.

## What we need back

**One screenshot of the sign-off table in section 5.** That is it. It reports `READY`,
`READY_WITH_CAVEATS` or `NOT_READY`, and lists anything outstanding with the action against
it.

Please send it the same day. If anything comes back as a BLOCKER, two of the possible ones
need a platform administrator and take days rather than minutes to clear, which is the whole
reason we are doing this a week early rather than on the morning.

## Then run the tutorial smoke test

Once the sign-off table looks right, open **`07_tutorial_smoke_test`**, set the same catalog,
schema and volume, and **Run All**. It takes about a minute.

This runs every query the morning tutorial asks attendees to run and prints one pass or fail
line each, ending in `TUTORIAL_OK` or `TUTORIAL_BROKEN`. It writes nothing.

The point is that passing in our workspace does not mean passing in yours. Metric views need
Databricks Runtime 16.4 or above and the document parsing needs 17.3 or above, so this tells
us which parts of the morning will work on your compute before twenty-one people try them.
One warning is expected if the runtime is below 17.3, and nothing else in the morning depends
on it.

Please send this screenshot too.

## Running it more than once

It is safe to re-run as often as you like. Every table is overwritten, every file directory
is cleared and rewritten, and the random seed is fixed, so the result is identical each
time. Re-running after a fix is how we confirm the fix worked.

## Two things the notebook cannot check

These need the platform owner either way, so they are worth raising in parallel rather than
waiting for the run to finish:

1. **A pro or serverless SQL warehouse.** Genie Agents do not run on a classic warehouse at
   all. Pro is not serverless: it still runs in the Ageas cloud account, so the infosec
   position on serverless does not apply to a change of warehouse type.
2. **Partner-powered AI features enabled at account and workspace level.** Genie does not
   work without it, and it is frequently switched off in a regulated estate.

## If something fails

Send us the error and the sign-off table as it stands. Do not try to work around it. The
notebook is designed to fail loudly and tell you who needs to fix what.
