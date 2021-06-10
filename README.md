# ManageInvite Database Client

Database client used by the [ManageInvite Discord BOT](https://github.com/manage-invite/manage-invite-bot) and by the [ManageInvite API](https://manage-invite/manage-invite-api).

### Yarn configuration

* Login. (`npm login --registry=https://npm.pkg.github.com --scope=@manage-invite`). Username is `manage-invite`, password is a valid personal access token.
* Create `.yarnrc` file.
* You're done.

#### .yarnrc

```yml
"@manage-invite:registry" "https://npm.pkg.github.com/"
```

#### Installation

```sh
yarn add @manage-invite/manage-invite-db-client
```
