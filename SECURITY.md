# Security

## Reporting

Report vulnerabilities privately through GitHub's security advisory form for this repository
rather than a public issue.

## Threat model in brief

The bot holds a Personal Access Token (PAT). Rocket.Chat authorizes every API call as the bot,
not as the human who asked Hermes to do something, so the plugin is a confused-deputy risk by
construction and adds its own authorization layer in front of every credentialed action.

- **Inbound admission.** The gateway's allowlist (`ROCKETCHAT_ALLOWED_USERS`, pairing approvals,
  `GATEWAY_ALLOWED_USERS`) is consulted before the adapter downloads attachments, runs ffmpeg,
  fetches thread history, forwards slash commands, or writes topics. Unauthorized senders get a
  text-only dispatch so the runner can offer pairing; nothing else happens for them.
- **Peer bots.** Messages flagged as bot-generated are ignored unless they are a delegation task
  envelope. A delegated task may run an agent turn but can never execute gateway control
  commands (`/restart`, `/update`, `/sethome`, `/model` ...). Replies into a delegated DM carry
  a terminal result envelope so two agents cannot loop.
- **Agent tools.** Read tools are scoped to the current room by default; cross-room reads and every
  write require exact allowlists plus a trusted requester, resolved from Hermes' task-local session
  context only (never process-global `HERMES_SESSION_*` values). Writes, local file uploads, slash
  forwarding, and topic writes are separate default-off capabilities.
- **Host files.** Uploads read only below configured roots through descriptor-relative opens
  (`O_NOFOLLOW` on every component), reject symlinks and traversal, and are size-bounded.
- **Network.** HTTPS is required unless explicitly disabled. The PAT is sent only to the configured
  origin: API redirects are refused, attachment redirects to object storage are followed once
  without credentials and only to public HTTPS hosts unless
  `ROCKETCHAT_ALLOW_PRIVATE_FILE_REDIRECTS=true`. Response bodies, DDP frames, media downloads,
  thread context, and tool results are all bounded.
- **Republished messages.** Rocket.Chat re-broadcasts a message document on every mutation. The
  adapter classifies such frames structurally and by server timestamps and never treats them as a
  new instruction from the user.
- **Logging.** Audit lines carry keyed hashes of room and user ids, never message text or tokens.
- **Profiles.** Credentials and allowlists are read through Hermes' profile-scoped secret reader,
  and one PAT is locked to one running gateway.

Retrieved chat text can still contain prompt injection. Keep write tools disabled for research-only
deployments and require human review for consequential actions.
