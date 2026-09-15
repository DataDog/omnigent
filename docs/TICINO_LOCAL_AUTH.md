# Local Ticino authentication

This setup runs Omnigent on the host, uses Ticino's staging issuer for
human login, and stores normal Omnigent application state in a local Docker
Postgres. It does not enable Habitat, on-behalf-of token exchange, or durable
storage of Ticino tokens.

## 1. Register the public OAuth client

Create `oauth_client.omnigent-local` in the `ticino` Balto domain through
[Mosaic](https://mosaic.us1.ddbuild.io/balto/domains/ticino) with the following
value, then distribute it to **Control Plane Staging**. The ordinary Staging
target does not include `us1.ddbuild.staging.dog`.

```json
{
  "client_id": "omnigent-local",
  "client_name": "Omnigent Local Development",
  "redirect_uris": ["http://127.0.0.1:6767/auth/callback"],
  "grant_types": ["authorization_code", "refresh_token"],
  "response_types": ["code"],
  "scope": "openid email profile",
  "client_type": "public",
  "token_endpoint_auth_method": "none",
  "consent_mode": "explicit"
}
```

The scope must be non-empty. The redirect URI must match exactly, including
the `127.0.0.1` host and port.

## 2. Start Postgres

```bash
docker run -d \
  --name omnigent-ticino-postgres \
  -p 127.0.0.1:55432:5432 \
  -e POSTGRES_PASSWORD=omnigent-local \
  -e POSTGRES_DB=omnigent \
  -v omnigent-ticino-postgres:/var/lib/postgresql/data \
  postgres:16-alpine
```

On later runs, start the existing container with:

```bash
docker start omnigent-ticino-postgres
```

## 3. Configure Omnigent

Run these commands from the Omnigent worktree:

```bash
export OMNIGENT_AUTH_ENABLED=1
export OMNIGENT_AUTH_PROVIDER=oidc
export OMNIGENT_OIDC_ISSUER=https://ticino.identity.local-cluster.local-dc.fabric.dog:8443/v1/issuer/sycamore
export OMNIGENT_OIDC_CLIENT_ID=omnigent-local
unset OMNIGENT_OIDC_CLIENT_SECRET
export OMNIGENT_OIDC_REDIRECT_URI=http://127.0.0.1:6767/auth/callback
export OMNIGENT_OIDC_SCOPES='openid email profile'
export OMNIGENT_OIDC_ALLOWED_DOMAINS=datadoghq.com
export OMNIGENT_OIDC_SKIP_EMAIL_VERIFICATION=1
export OMNIGENT_OIDC_COOKIE_SECRET="$(openssl rand -hex 32)"

export OMNIGENT_OIDC_AUTHORIZATION_ENDPOINT=https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/oauth/authorize
export OMNIGENT_OIDC_TOKEN_ENDPOINT=https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/oauth/token
export OMNIGENT_OIDC_JWKS_URI=https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/.well-known/keys
```

The canonical issuer stays separate from the public endpoint host because
Ticino signs ID tokens with its Fabric issuer. Omnigent validates that issuer
strictly while using the public URLs for browser and HTTP transport.

`OMNIGENT_OIDC_SKIP_EMAIL_VERIFICATION=1` is required because Ticino's signed
user claims contain `email` but not `email_verified`. Keep the
`datadoghq.com` domain allowlist when using this setting.

## 4. Run and verify

Install the optional Postgres driver into the worktree environment once:

```bash
uv pip install --python .venv/bin/python 'psycopg[binary]>=3.1,<4'
```

Then start the server:

```bash
uv run omnigent server \
  --host 127.0.0.1 \
  --port 6767 \
  --database-uri postgresql+psycopg://postgres:omnigent-local@127.0.0.1:55432/omnigent \
  --no-open
```

Open <http://127.0.0.1:6767>. The login button should redirect through Ticino
and Google, then return to `/auth/callback` and set the local `ap_session`
cookie. Confirm `/v1/me` returns your lowercase Datadog email after login.
