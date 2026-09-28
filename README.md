# Signal Desk on Vercel

This is the public, registration-free edition. The website is static. Every visitor's watchlist and hidden collections stay in that browser's local storage. The scanner runs in GitHub Actions twice per hour and commits a fresh public snapshot. A Vercel Deploy Hook publishes that snapshot.

## Publish

1. Create a GitHub repository and put **the contents of this folder** at its root, including `.github/workflows/scan.yml`. A personal repository is simplest for a Vercel Hobby account.
2. In [Vercel](https://vercel.com/new), import the GitHub repository. Choose **Other** for Framework Preset. The included `vercel.json` sets the Output Directory to `public`. Deploy it. The initial deployment uses the included snapshot.
3. In the Vercel project's **Settings → Git → Deploy Hooks**, create a hook for the production branch. Copy its URL. Treat it as a secret.
4. In GitHub's repository **Settings → Secrets and variables → Actions**, add a repository secret named `VERCEL_DEPLOY_HOOK` containing that URL. Allow GitHub Actions to write repository contents if your repository settings restrict workflow permissions.
5. In GitHub **Actions → Refresh mint feed**, use **Run workflow** once. The scheduled job then runs at minutes 17 and 47 of each UTC hour. Check both the Actions run and the Vercel deployment.

Vercel assigns a `*.vercel.app` URL. You can add a custom domain later.

## Local preview

Run `python -m http.server 8000 --directory public`, then open `http://localhost:8000`. A local refresh of the public snapshot is `python scan.py --mode quick`. The scanner uses only the Python standard library.

## Public behavior

- Visitors can search the latest scanned collections, watch them, hide them, and import or export personal preferences without an account.
- **Find collection** searches the published snapshot. New collections appear after a scheduled scan; visitors cannot force a network scan.
- Browser notifications require the page to remain open.
- Local storage belongs to a browser profile and origin. Clearing site data removes personal preferences unless the visitor exported a backup. Automatic cross-device sync would require an optional recovery token and a database.
- GitHub Actions schedules can be delayed or skipped. The site marks data as stale after 45 minutes.

If a scan fails or produces too few collections, the workflow exits before committing. The last published snapshot stays online. The Deploy Hook is used because Vercel Hobby can reject Git deployments authored by `github-actions[bot]`.
