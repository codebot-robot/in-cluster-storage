# structured-data-streams.com

The website for Structured Data Streams (SDS): SQL, OLTP, OLAP, and filesystems as snapshots over an authoritative stream of proto records.

Built with [Hugo](https://gohugo.io/), deployed to GitHub Pages by `.github/workflows/site.yml`.

## Local Preview

Requires Hugo extended v0.165.0 or later.

```bash
cd site
make preview        # runs local server at http://localhost:1313/
make build          # builds the site into public/
```

## Structure

- `content/`: Markdown content pages (`idea.md`, `format.md`, `projections.md`, `case-study.md`, `faq.md`, `implementations.md`).
- `layouts/`: Hugo HTML templates (`baseof.html`, `home.html`, `page.html`, `404.html`, `_partials/`).
- `assets/css/`: Stylesheets (`site.css`, `landing.css`).
- `static/`: Static assets (favicon, images).

## Domains & Deployment

- Base URL defaults to `https://structured-data-streams.com/` in `hugo.toml`.
- GitHub Pages must be configured with the **GitHub Actions** build and deployment source in repository settings (**Settings → Pages → Build and deployment → Source: GitHub Actions**).
- A custom domain is configured directly in repository settings under **Settings → Pages → Custom domain**, plus DNS records at the registrar. When publishing via a GitHub Actions workflow, GitHub manages custom domain routing through repository settings and ignores any `CNAME` file.
- The site deploys automatically on any push to `main` touching `site/**` or `.github/workflows/site.yml`, or can be triggered manually via `gh workflow run site.yml --ref main`.
- Pull request builds compile against a subpath base URL and run an automated link check to ensure all internal navigation links preserve subpaths.
