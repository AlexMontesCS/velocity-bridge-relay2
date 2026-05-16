# Velocity Bridge Cloudflare Relay

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/AlexMontesCS/Velocity-Bridge/tree/main/relay-cloudflare)

This relay runs as a Cloudflare Worker with one Durable Object per pair ID.
Messages are stored inside that pair's Durable Object, and desktop delivery uses
Server-Sent Events. The `GET /messages/...` endpoint remains available for
manual debugging and older clients.

## Why Cloudflare?

Deno KV can read recently queued messages from one edge while a later laptop poll
lands on another edge and sees nothing. Durable Objects avoid that class of bug:
all traffic for a pair ID is routed to the same object, so cellular phone sends
and laptop SSE reads share one consistent state.

Cloudflare's Workers free tier includes Workers and Durable Objects usage that is
enough for personal clipboard syncing.

## Deploy

```sh
cd relay-cloudflare
npm install
npm run deploy
```

Copy the deployed Worker URL, such as:

```text
https://velocity-bridge-relay.<your-subdomain>.workers.dev
```

Paste that URL into Velocity Bridge's relay settings and into the iPhone
Shortcut's relay URL field.

## API

The Cloudflare relay keeps the same endpoints as the Deno relay:

- `GET /`
- `POST /v1/pairs/{pairId}/phone/send`
- `POST /v1/pairs/{pairId}/phone/request_clipboard`
- `GET /v1/pairs/{pairId}/phone/latest_clipboard`
- `POST /v1/pairs/{pairId}/messages/{desktop|phone}`
- `GET /v1/pairs/{pairId}/messages/{desktop|phone}`
- `GET /v1/pairs/{pairId}/subscribe/{desktop|phone}`

Messages expire after 24 hours by default. Set `MESSAGE_TTL_SECONDS` in
`wrangler.jsonc` if you want a different local retention window.

## Troubleshooting 1010 Access Denied

If the desktop app reports `Cloudflare 1010` or `browser_signature_banned`, the
request is being blocked by Cloudflare before it reaches the Worker. For an API
relay, disable **Browser Integrity Check** for this Worker route:

1. Open the Cloudflare dashboard.
2. Select the zone/account used by the relay.
3. Go to **Security -> Settings** and turn off **Browser Integrity Check**, or
   create a WAF custom rule that skips Browser Integrity Check for the relay
   hostname.

The desktop client sends a `VelocityBridge/...` user agent, but Cloudflare's
Browser Integrity Check can still reject non-browser API clients when enabled.
