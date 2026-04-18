# systemd deployment for wa-greenapi-ingest-skill

This directory is the tracked source of truth for the production scheduler used on `rm.loc`.

Why this exists:

- `greenapi_ingest.py` and `embed_missing.py` are deterministic shell/Python jobs
- OpenClaw `cron` agent turns add an unnecessary LLM hop before those commands run
- `systemd` timers remove that waste while keeping cadence versioned in the skill repo

Files:

- `*.service.template`: oneshot services that execute the tracked runner scripts
- `*.timer.template`: cadence definitions
- `install-wa-greenapi-timers.sh`: renders templates with the live skill path and enables timers
- `uninstall-wa-greenapi-timers.sh`: disables and removes installed units

Credential inheritance:

- the rendered services import `WA_GREENAPI_GATEWAY_ENV_FILE` during install
- default: `/etc/openclaw/openclaw.env`
- the rendered services must keep the host OpenClaw path split:
  - `OPENCLAW_HOME=/home/openclaw`
  - `OPENCLAW_STATE_DIR=/home/openclaw/.openclaw`
- this lets the jobs reuse the same provider credentials that `openclaw.service` already has
- the audio fallback path also reuses the host OpenClaw CLI config via `openclaw capability audio transcribe`
- if direct `OPENAI_API_KEY` is absent, the skill can still fall through to local whisper and then to the host OpenClaw `tools.media.audio` chain

Install or refresh:

```bash
cd /path/to/wa-greenapi-ingest-skill
sudo ./ops/systemd/install-wa-greenapi-timers.sh
```

Verify:

```bash
systemctl list-timers 'wa-greenapi-*'
systemctl status wa-greenapi-ingest-queue.timer --no-pager
journalctl -u wa-greenapi-enrich-media.service -n 100 --no-pager
```

Current enrich defaults:

- `wa_enrich_media_docs_audio.sh` runs with `WA_GREENAPI_ENRICH_MAX_EVENTS=8` unless overridden
- keep this job in small batches; it is deterministic, but the final audio fallback can still hit a shared provider path through `openclaw capability audio transcribe`
- `greenapi_ingest.py` preserves an already successful audio transcript if a later retry for the same `source_message_id` fails, so timer retries cannot downgrade the archive back to `transcript_unavailable`

Change schedule later:

1. Edit the tracked `*.timer.template` files in this repo.
2. Commit and deploy the repo update.
3. Re-run `sudo ./ops/systemd/install-wa-greenapi-timers.sh`.

Operational rule:

- do not recreate OpenClaw `cron` agent-turn wrappers for these jobs
- if OpenClaw needs to change the cadence later, it should edit these tracked templates and redeploy them
