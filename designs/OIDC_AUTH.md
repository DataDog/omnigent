# Habitat broker sessions

The existing Habitat backend selection also selects broker-backed OIDC login.
Set `OMNIGENT_SANDBOX_PROVIDER_MODULE=omnigent_hab_launcher` and `OMNIGENT_HAB_ENABLED=1`.
`HAB_APISERVER` identifies the Habitat HTTPS API origin.
Other sandbox providers keep the existing OIDC token path.

Omnigent calls Habitat `AuthService` with the pod application bearer.
The Habitat `omnigent` profile uses the existing OAuth client and callback URI.
Habitat must approve the exact application actor before deployment.
Omnigent does not infer that approval from the OAuth client name.

The broker owns provider refresh tokens and OAuth PKCE transactions.
Omnigent stores only an encrypted broker session credential in its existing OIDC session row.
Omnigent retrieves a current ID token for each delegated operation.
The existing external OBO exchange still produces the Habitat API bearer.
The plugin sends the broker session credential only on `CreateHab` as `x-hab-credential-session`.
Logout revokes the broker session before it revokes the local session.

Deploy Habitat support and the compatible plugin wheel before this application change.
The source image must pin both the application commit and the dd-source plugin commit.
Existing Habitat users must sign in again to obtain a broker session.
CLI and device flows request `force_reauthentication` through `StartLogin`.
The broker must support that field before this application change reaches production.
