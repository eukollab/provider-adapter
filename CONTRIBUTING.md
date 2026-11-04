# Contributing

Thank you for helping. Issues and pull requests are welcome here.

## How a change gets in

This repository is published from EuKreate's own repository, where every change is tested together with the rest of
EuKollab. So a pull request is not merged here. When we accept it, a maintainer applies it there, crediting you in the
commit, and it comes back here with the next release. We then close the pull request with a link to that release.

## Sign off your commits

Every commit needs a `Signed-off-by` line with your name and email, certifying the
[Developer Certificate of Origin](https://developercertificate.org): that you wrote the change or have the right to
submit it under this repository's licence. `git commit -s` adds the line. A check on the pull request asks for it;
there is no agreement to sign.

## Run the tests

`.github/workflows/tests.yml` runs them on every pull request; run the same commands locally first. In
`eukollab/provider-adapter`, the tests read the protocol's schemas and vectors from a checkout of `eukollab/ekp`:

```bash
git clone https://github.com/eukollab/ekp .ekp
pip install -e ".[dev]"
EKP_DIR=$PWD/.ekp pytest
(cd ts && EKP_DIR=$PWD/../.ekp node --test) && (cd agent-ts && node --test)
```

## Before you open a pull request

- For anything larger than a fix, open an issue first, so we can agree on the approach.
- Keep a pull request to one change, and say what it fixes or adds and how you tested it.
- Never include credentials, keys or tokens, even expired ones.

Security problems are not reported here: see [SECURITY.md](SECURITY.md).
